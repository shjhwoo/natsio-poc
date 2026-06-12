# JetStream stream seq를 버전 번호로 써도 되는가 PoC

14강 보강용 PoC. 순서 역전 방어/낙관적 락에서 `incoming_version > stored_version` 비교에 사용할 버전 소스로 JetStream stream seq를 써도 되는지, Purge·delete/recreate·타임스탬프 대안을 NATS 2.12.2 + nats.go 기준으로 실측한다.

## 검증 목적

순서 역전 방어는 들어온 이벤트가 저장된 이벤트보다 더 최신인지 비교해 오래된 이벤트를 무시한다. 이 비교 기준값을 무엇으로 삼을지가 핵심이다.

후보:

1. 앱이 매기는 per-entity version: DB 행의 version 컬럼을 +1. 의미상 가장 정확하지만 발행 시 직전 버전을 읽어야 한다.
2. JetStream stream seq: 서버가 메시지마다 매기는 단조 증가 번호. 편리하지만 스트림 수명에 묶인다.
3. 타임스탬프: 스트림 수명과 무관하지만 클록 스큐, 동일 해상도 동률, 역행 위험이 있다.

이 PoC는 (2)가 Purge와 delete/recreate에서 어떻게 동작하는지, (3)이 lifecycle 리셋 문제를 피하면서도 어떤 클록 한계를 갖는지 확인한다.

## 환경

| 항목 | 값 |
|---|---|
| NATS Server | 2.12.2 (`nats:2.12.2`) |
| nats.go | v1.52.0 |
| Go | go1.26.2 |
| Storage | FileStorage 주 검증 + MemoryStorage 교차 검증 |
| 버전 비교 로직 | `incoming > stored`이면 반영, 아니면 무시 |
| stream seq 확인 | `js.Publish`의 PubAck `Sequence` 및 `StreamInfo.State.{FirstSeq,LastSeq}` |
| subject | File: `order.updated.file`, Memory: `order.updated.memory`, Timestamp: `order.updated.ts` |

## 실행 방법

```bash
go build -o jetstream-seq-version-poc.exe .
docker compose up -d
./jetstream-seq-version-poc.exe | tee run-output.txt
```

실제 실행 로그는 `run-output.txt`에 저장했다.

## Part 1 — JetStream stream seq의 Purge·재생성 거동

### Phase S-1 — 정상 단조 증가 확인

| 항목 | 내용 |
|---|---|
| 시나리오 | `order.updated.*`를 10개 발행 |
| 기대값 | seq가 `1,2,3,...,10`으로 단조 증가 |
| FileStorage 실제값 | `[1 2 3 4 5 6 7 8 9 10]` |
| MemoryStorage 실제값 | `[1 2 3 4 5 6 7 8 9 10]` |
| 결론 | 기준선 성립. stream seq는 스트림 수명 안에서 발행마다 단조 증가한다. |

### Phase S-2 — Purge 후 seq 초기화 여부

#### 전체 Purge

| 항목 | 내용 |
|---|---|
| 시나리오 | 1~10 발행 후 stream 전체 Purge, 다시 1개 발행 |
| 기대값 | 새 메시지 seq = 11 |
| FileStorage 실제값 | Purge 직후 `msgs=0 first_seq=11 last_seq=10`, 재발행 seq = 11 |
| MemoryStorage 실제값 | Purge 직후 `msgs=0 first_seq=11 last_seq=10`, 재발행 seq = 11 |
| 결론 | H1 성립. Purge는 메시지를 지우지만 last_seq를 초기화하지 않는다. |

#### `Purge(WithPurgeKeep(2))`

| 항목 | 내용 |
|---|---|
| 시나리오 | 1~10 발행 후 마지막 2개만 남기고 Purge, 다시 1개 발행 |
| 기대값 | 새 메시지 seq = 11 |
| FileStorage 실제값 | Purge 직후 `msgs=2 first_seq=9 last_seq=10`, 재발행 seq = 11 |
| MemoryStorage 실제값 | Purge 직후 `msgs=2 first_seq=9 last_seq=10`, 재발행 seq = 11 |
| 결론 | Keep 옵션에서도 last_seq는 유지된다. |

#### `Purge(WithPurgeSequence(6))`

| 항목 | 내용 |
|---|---|
| 시나리오 | 1~10 발행 후 seq 6 미만을 Purge, 다시 1개 발행 |
| 기대값 | 새 메시지 seq = 11 |
| FileStorage 실제값 | Purge 직후 `msgs=5 first_seq=6 last_seq=10`, 재발행 seq = 11 |
| MemoryStorage 실제값 | Purge 직후 `msgs=5 first_seq=6 last_seq=10`, 재발행 seq = 11 |
| 결론 | Sequence 옵션에서도 last_seq는 유지된다. |

### Phase S-3 — 스트림 delete + recreate 후 seq

| 항목 | 내용 |
|---|---|
| 시나리오 | 1~10 발행 후 DeleteStream, 같은 이름/subject로 CreateStream, 다시 1개 발행 |
| 기대값 | 새 메시지 seq = 1 |
| FileStorage 실제값 | recreate 직후 `msgs=0 first_seq=0 last_seq=0`, 재발행 seq = 1 |
| MemoryStorage 실제값 | recreate 직후 `msgs=0 first_seq=0 last_seq=0`, 재발행 seq = 1 |
| 결론 | H2 성립. 스트림을 삭제 후 재생성하면 seq가 1로 리셋된다. |

### Phase S-4 — 실패 모드 재현: 저장된 version > 새 seq → 전건 무시

| 항목 | 내용 |
|---|---|
| 시나리오 | DB에 마지막 처리 version=10 저장. 스트림 delete/recreate 후 새 이벤트 3개 발행(seq 1,2,3). `incoming > stored` 비교 적용 |
| 기대값 | 새 이벤트 전부 무시 |
| FileStorage 실제값 | `seq=1 <= stored(10) => IGNORE`, `seq=2 <= stored(10) => IGNORE`, `seq=3 <= stored(10) => IGNORE` |
| MemoryStorage 실제값 | 동일. `applied=0 ignored=3 seqs=[1 2 3]` |
| 결론 | 치명적 silent failure 재현. 에러 없이 새 이벤트가 전건 무시될 수 있다. |

## Part 2 — 타임스탬프 기반

### Phase T-1 — Purge·재생성을 가로질러 단조성 유지되는가

| 항목 | 내용 |
|---|---|
| 시나리오 | payload에 UnixNano timestamp를 넣어 발행. Purge 후 발행, delete/recreate 후 발행 |
| 기대값 | payload timestamp는 스트림 lifecycle과 무관하므로 이전 값보다 큼 |
| 실제값 | `before=1781272088254892100 afterPurge=1781272088261597000 afterRecreate=1781272088270495900` |
| stream seq 참고 | delete/recreate 후 stream seq는 다시 1이지만 payload timestamp는 계속 증가 |
| 결론 | H3 일부 성립. 타임스탬프는 스트림 수명 리셋 문제를 받지 않는다. |

### Phase T-2 — 타임스탬프의 한계

#### 같은 millisecond/해상도 동률

| 항목 | 내용 |
|---|---|
| 시나리오 | DB 저장값과 incoming timestamp를 같은 millisecond 값으로 설정 |
| 실제값 | `stored=1781272088271 incoming=1781272088271 apply=false` |
| 결론 | `>` 비교에서는 동률을 최신 이벤트로 반영할 수 없다. 해상도가 낮거나 동일 tick에서 여러 이벤트가 나오면 순서를 못 가른다. |

#### 클록 스큐/역행

| 항목 | 내용 |
|---|---|
| 시나리오 | 실제로 더 늦게 온 이벤트의 payload timestamp를 5초 과거로 설정 |
| 실제값 | `stored=1781272088271659000 incoming=1781272083271659000 apply=false` |
| 결론 | 발행자 클록이 뒤처지거나 역행하면 실제 최신 이벤트도 오래된 것으로 오판되어 무시될 수 있다. |

## 결과 요약

| 검증 항목 | 실제 결과 | 가설 |
|---|---|---|
| Purge 후 seq | 전체 Purge/Keep/Sequence 모두 다음 발행 seq = 11 | H1 성립 |
| delete+recreate 후 seq | 다음 발행 seq = 1 | H2 성립 |
| 재생성 시 실패 모드 | stored=10 상태에서 새 seq 1,2,3 전건 무시 | 재현됨 |
| 타임스탬프 lifecycle | Purge/delete+recreate와 무관하게 payload timestamp 증가 | H3 lifecycle 장점 성립 |
| 타임스탬프 클록 한계 | 동일 ms 동률, 5초 과거 스큐 모두 `>` 비교에서 반영 실패 | 한계 재현됨 |

## 14강 반영 결론

1. `per-entity version`이 가장 안전한 기준이다. 엔티티의 논리적 변경 순서를 직접 표현하기 때문이다. 단, 발행 시 직전 버전을 읽거나 DB write와 함께 version을 증가시키는 설계가 필요하다.
2. JetStream stream seq는 스트림 수명 안에서는 편리하고 단조 증가한다. NATS 2.12.2 기준 Purge는 seq를 1로 되돌리지 않으므로 “Purge 때문에 버전이 리셋된다”는 걱정은 기각된다.
3. 진짜 위험은 stream delete/recreate, 마이그레이션, DR, 운영 실수처럼 스트림 자체가 새로 만들어지는 경우다. 이때 stream seq가 1부터 다시 시작해 DB에 저장된 마지막 seq보다 낮아지고, `incoming > stored` 비교는 새 이벤트를 조용히 전건 무시한다.
4. 타임스탬프는 스트림 수명과 분리되어 seq 리셋 문제는 피하지만, 클록 스큐·동률·역행이라는 별도 약점을 가진다. 단일 발행자, 충분한 해상도, NTP/monotonic discipline이 전제될 때만 차선책으로 쓸 수 있다.

한 줄 결론: “타임스탬프가 제일 안전”은 절반만 맞다. 가장 안전한 것은 엔티티가 들고 가는 per-entity version이고, JetStream seq는 스트림 수명, 타임스탬프는 클록이라는 서로 다른 약점을 가진다.

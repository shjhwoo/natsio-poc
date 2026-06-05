# 운영 중 Consumer의 FilterSubjects 런타임 변경 PoC — NATS 2.14 Consumer Reset API

## 검증 목적

NATS 2.14에서 도입된 **Consumer Reset API**(`$JS.API.CONSUMER.RESET.<STREAM>.<CONSUMER>`)를 사용해,
2.12.2에서 확인한 "완전 교체 시 AckFloor 오염으로 인한 데이터 손실" 문제가 해결되는지 검증한다.

> **2.12.2 PoC 핵심 발견 (비교 기준)**
> - 추가: 새로 추가된 subject의 AckFloor 이전 과거 메시지 skip
> - 제거: 안전. pending 즉시 해소. ack 위치 무영향.
> - **완전 교체 ★**: 업데이트 성공하나 AckFloor(stream global seq) 미리셋 → 새 subject 메시지 일부 영구 손실
>
> 파일: `readme-nats-2.12.2.md`

---

## 환경

| 항목               | 값                                                                         |
| ------------------ | -------------------------------------------------------------------------- |
| NATS Server        | **2.14** (`nats:2.14` Docker 이미지)                                        |
| nats.go 클라이언트 | v1.52.0 — Reset API 고수준 래퍼 없음, `nc.Request` 저수준 호출로 검증     |
| Retention Policy   | LimitsPolicy                                                               |
| Storage            | MemoryStorage                                                              |
| DeliverPolicy      | **DeliverAllPolicy** — 2.12.2와 동일 조건 유지                             |

> ⚠️ Consumer Reset API는 NATS **2.14+** 에서만 동작. 2.12.x에서 호출하면 에러.

### docker-compose.yaml 변경 사항

```yaml
# 변경 전
image: nats:2.12.2
# 변경 후
image: nats:2.14.0   # 또는 nats:latest
```

---

## Consumer Reset API 동작 원리 (ADR-60 기반)

서버 API 엔드포인트: `$JS.API.CONSUMER.RESET.<STREAM>.<CONSUMER>`

| 페이로드            | AckFloor 변화                                  | 결과                                            |
| ------------------- | ---------------------------------------------- | ----------------------------------------------- |
| `nil` (빈)          | AckFloor.Stream **유지**                        | pending·redelivery만 초기화                      |
| `{"seq": N}` 지정   | AckFloor.Stream → **N-1** (N 직전으로 이동)     | N 이상의 seq부터 재전달 시작                     |
| `{"seq": 0}` 또는 1 | AckFloor.Stream → **0** (처음으로 리셋)         | 필터 일치 메시지 전체 재전달 (이미 acked 포함!)  |

> **핵심 주의**: Reset은 "이미 acked된 메시지도 재전달 대상"으로 만든다.
> (ADR-60: "pending and redelivered messages are always reset")
> seq=0 reset 후 filter가 그대로라면 이전에 acked한 메시지도 다시 온다.
> **filter 변경 후 reset** 조합에서는 새 filter에 안 걸리는 subject는 어차피 전달 안 되므로 안전.

nats.go 고수준 API에 Reset 미지원 시 저수준 호출:
```go
// seq 지정 reset
nc.Request("$JS.API.CONSUMER.RESET.FILTER_TEST.d3-test",
    []byte(`{"seq":1}`), 5*time.Second)

// seq=0 reset (처음부터)
nc.Request("$JS.API.CONSUMER.RESET.FILTER_TEST.d3-test",
    []byte(`{"seq":0}`), 5*time.Second)
```

---

## 공통 셋업

```
Stream: FILTER_TEST
Subjects: test.a, test.b, test.c  |  Retention: LimitsPolicy  |  Storage: MemoryStorage
발행: 5라운드 인터리브 (a1,b1,c1,...,a5,b5,c5)  — 2.12.2와 동일 조건
인터리브 seq 예시: a=1,4,7,10,13 | b=2,5,8,11,14 | c=3,6,9,12,15
  (Purge는 seq를 리셋하지 않으므로 실제 seq는 phase마다 다름)
```

---

## Phase R-1 ★ — 완전 교체 + Reset  (`[a]` → `[b]`)

2.12.2에서 데이터 손실이 발생한 핵심 케이스. Reset API 적용 시 해결되는지 확인.

|                  |                                                                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | Durable(filter=[a], DeliverAll) 생성 → test.a 3개 ack(AckFloor ≈ seq 7) → filter [b]로 교체 → **Reset with `{"seq":1}`** → 수신 확인               |
| **확인 포인트**  | ① Reset 후 AckFloor.Stream이 0으로 변경되는가 ② test.b 메시지를 **5개 전부** 받는가(2.12.2에서는 3개만) ③ 이미 acked한 test.a 메시지가 재전달되는가 |
| **기대값(가설)** | ① AckFloor → 0 ② b 5개 전부 수신 — filter=[b]이므로 a 재전달 없음 ③ a는 filter에 안 걸려 재전달 없음 → 안전                                        |
| **2.12.2 비교**  | 2.12.2: b 3개만 수신(b-001,b-002 skip) → Reset으로 5개 전부 수신 가능하면 문제 해결 확인                                                           |
| **실제값**       | AckFloor 변화: **7 → 7 → 0** (filter 변경 후에도 AckFloor 유지, Reset seq=1 호출 후 0으로 리셋). NumPending: 2 → 3 → **5**. b 수신: **5개 전부** (b-001~005, seq 2,5,8,11,14). a 재전달: **0개** (filter=[b]가 차단). |
| **결론**         | **Reset API로 완전 교체 시 데이터 손실 문제 해결.** `filter 변경 → Reset(seq=firstSeq)` 두 단계로 AckFloor를 0으로 리셋하면 새 subject의 모든 메시지를 처음부터 수신 가능. filter가 새 subject만 허용하므로 이전 subject 재전달 없음. 2.12.2: b 3개(손실 2개) → **2.14+Reset: b 5개(손실 없음) ★** |

---

## Phase R-2 — 추가 + Reset  (`[b, c]` → `[a, b, c]`)

2.12.2에서 추가된 a의 AckFloor 이전 메시지가 skip됐던 케이스.
Reset 적용 시 a 전체 수신이 가능해지나, **이미 acked한 b,c가 재전달되는 부작용**이 있을 수 있다.

|                  |                                                                                                                                                                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | Durable(filter=[b,c], DeliverAll) → b,c 5개 ack(AckFloor ≈ seq 8) → filter [a,b,c]로 추가 → **Reset with `{"seq":1}`** → 수신 확인                                                                                                          |
| **확인 포인트**  | ① Reset 후 a 메시지를 **5개 전부** 받는가(2.12.2에서는 2개만) ② 이미 acked한 b,c 메시지가 재전달되는가 ③ 재전달 범위: seq 1부터의 b,c 전부인가, 아니면 AckFloor 이후만인가                                                                   |
| **기대값(가설)** | ① a 5개 전부 수신 ② **b,c 이미 acked 메시지도 재전달 발생** — filter=[a,b,c]이고 seq=1부터 reset이므로 acked된 b1(seq2),c1(seq3),... 도 재전달될 것 ③ 이 부작용이 추가 케이스에서 Reset의 한계. 실무에서는 중복 처리 방지 로직 필요할 수 있음 |
| **2.12.2 비교**  | 2.12.2: 추가는 성공하나 a-001~003 skip. Reset으로 a 전부 받되 b,c 중복 감수 필요                                                                                                                                                             |
| **실제값**       | AckFloor 변화: **23 → 23 → 15** (filter 변경 후 AckFloor 유지, Reset seq=16 후 15로 리셋). NumPending: 5 → 7 → **15**. 수신: a=5, b=5, c=5 총 **15개 전부** 재전달됨. — b-001~003(이미 acked), c-001~002(이미 acked) 포함 전부 재전달. |
| **결론**         | a 5개 전부 수신 성공. **단, Reset은 이미 acked된 b,c 메시지도 전량 재전달시킨다.** 총 15개(스트림 전체)가 다시 왔으며, 그 중 5개는 이전에 acked했던 b,c 메시지. 추가 케이스에서 Reset을 쓰면 **중복 수신이 반드시 발생**하므로 idempotent consumer(중복 처리 방지) 구현이 전제되어야 한다. "과거 a 메시지 수신이 필요 없다면 Reset을 쓰지 않는 것이 안전." |

---

## Phase R-3 — 제거 (Reset 불필요, 대조군)  (`[a, b, c]` → `[a, b]`)

2.12.2에서도 안전했던 케이스. 2.14에서도 동일한 거동인지 확인. Reset 미사용.

|                  |                                                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | Durable(filter=[a,b,c], DeliverAll) → a,b,c 각 2개씩 ack → filter [a,b]로 제거 → Reset 없이 수신 확인              |
| **확인 포인트**  | ① NumPending 즉시 재계산(c pending 제거)되는가 ② a,b ack 위치 무영향인가 ③ 2.12.2와 동일 거동인가                   |
| **기대값(가설)** | 2.12.2와 동일. 제거는 Reset 없이도 안전.                                                                            |
| **실제값**       | NumPending: 9 → **6** (차이=3, c pending 즉시 제거). a=3, b=3, c=**0** 수신. AckFloor 유지. |
| **결론**         | 2.12.2와 완전히 동일한 거동. 제거는 2.14에서도 Reset 없이 안전하며 변화 없음.               |

---

## Phase R-4 — Reset seq 정밀 제어 (세밀 검증)

빈 페이로드 / seq 지정 / seq=0 reset의 AckFloor 변화 차이를 수치로 확인.

|              |                                                                                                                                                                       |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **시나리오** | Durable 생성 → 일부 ack → 세 가지 reset 방식 각각 적용 후 ConsumerInfo 스냅샷 비교                                                                                    |
| **측정값**   | AckFloor.Stream, NumPending (reset 전/후)                                                                                                                             |
| **목적**     | ① 빈 페이로드: AckFloor 유지 확인 ② `{"seq": N}`: AckFloor → N-1 확인 ③ `{"seq": 0}`: AckFloor → 0 확인 ④ Named Ephemeral에서도 동일하게 동작하는가                  |
| **실제값**   | **케이스 A (빈 페이로드)**: AckFloor 52 → **52 유지**, NumPending 변화 없음. **케이스 B (seq=61 지정)**: AckFloor 67 → **60 (= seq-1)**, NumPending 2 → 5. ADR-60 문서 동작 수치로 확인. |

---

## 결과 요약 템플릿 (실험 후 채움)

| 케이스                                          | 2.12.2 결과                        | 2.14 + Reset 결과                            | 부작용                                               |
| ----------------------------------------------- | ---------------------------------- | -------------------------------------------- | ---------------------------------------------------- |
| 완전 교체 ([a]→[b]) + Reset `{"seq":firstSeq}`  | b **3개** (2개 영구 손실)          | b **5개** — 손실 없음 ★                      | **없음** (이전 subject는 filter로 차단)              |
| 추가 ([b,c]→[a,b,c]) + Reset `{"seq":firstSeq}` | a **2개** (3개 skip)               | a **5개** — 전부 수신                        | **acked b,c 전량 재전달** (총 15개 수신, 중복 10개)  |
| 제거 ([a,b,c]→[a,b]) — Reset 불필요             | 안전 (c pending 즉시 제거)         | **동일** — Reset 불필요, 변화 없음           | 없음                                                 |
| Reset 빈 페이로드 (nil)                          | N/A                                | AckFloor **유지**, pending·redelivery만 초기화 | -                                                    |
| Reset `{"seq":N}` 지정                           | N/A                                | AckFloor → **N-1** (수치 확인됨)             | -                                                    |

---

## 강의(13-A강)에 반영할 결론 (확정)

**2.14 Consumer Reset API로 완전 교체의 데이터 손실 문제는 해결된다. 단 케이스별 주의사항이 다르다.**

**완전 교체 ([a]→[b]) — Reset 사용 권장**
- `filter 변경 → Reset(seq=firstSeq)` 두 단계로 AckFloor를 0으로 리셋
- 새 subject(b)의 모든 메시지를 처음부터 수신. **데이터 손실 없음.**
- 이전 subject(a)는 filter가 차단하므로 재전달 없음 — 부작용 없음
- 주의: filter 변경과 Reset 사이에 짧은 시간 간격이 있음. 그 사이 유입된 메시지는 NumPending에 정상 포함됨 (문제 없음)

**추가 ([b,c]→[a,b,c]) — Reset 사용 시 중복 발생**
- Reset을 쓰면 a 전부 수신 가능하지만 **이미 acked한 b,c도 전량 재전달됨**
- "추가된 subject의 과거 메시지가 필요 없다면" → Reset 불필요. 2.12.2와 동일하게 AckFloor 이후 메시지만 수신
- "과거 메시지가 필요하다면" → Reset 사용 + **idempotent consumer(중복 처리 방지) 필수**

**제거 ([a,b,c]→[a,b]) — Reset 불필요**
- 2.12.2, 2.14 모두 안전. pending 즉시 해소. Reset 없이도 완벽 동작.

---

## 실행 메모

- NATS Server: `nats:2.14` Docker 이미지 사용
- nats.go v1.52.0: `ResetConsumer` 고수준 API 없음 → `nc.Request("$JS.API.CONSUMER.RESET.<STREAM>.<CONSUMER>", payload, timeout)` 직접 호출로 검증
- Reset seq 지정: `{"seq": firstSeq}` — Purge 후 첫 seq를 `stream.Info().State.FirstSeq`로 동적 획득. Purge는 seq 카운터를 리셋하지 않으므로 phase마다 값이 다름.
- 2.12.2 PoC와 동일한 인터리브 발행 패턴 유지 → 결과 직접 비교 가능
- 코드 위치: `jetstreamConsumerUpdate/v214/main.go`

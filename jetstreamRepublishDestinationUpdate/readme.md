# JetStream Republish Destination Live Update PoC

## 검증 목적

운영 중인 JetStream stream의 `RePublish` 설정을 `CreateOrUpdateStream()` 으로 변경해서,
그 이후 새로 들어오는 메시지가 다른 destination subject로 republish 되도록 바꿀 수 있는지 검증한다.

핵심 질문:

1. 기존에 `RePublish -> bridge.v1.>` 로 동작 중인 stream을 운영 중에 `bridge.v2.>` 로 바꿀 수 있는가?
2. 변경 직후 새로 들어오는 메시지는 v2 쪽 stream으로만 들어가는가?
3. 변경 이전에 v1로 republish 된 메시지가 소급해서 v2로 이동하지는 않는가?

## 환경

| 항목 | 값 |
|---|---|
| NATS Server | 2.12.2 |
| nats.go | v1.52.0 |
| source stream | `SRC`, subjects=`source.events.>` |
| target stream v1 | `TGT_V1`, subjects=`bridge.v1.>` |
| target stream v2 | `TGT_V2`, subjects=`bridge.v2.>` |
| source stream 초기 republish | `{Source: ">", Destination: "bridge.v1.>"}` |
| 변경 후 republish | `{Source: ">", Destination: "bridge.v2.>"}` |

## 실행 방법

```bash
go mod tidy
go build -o jetstreamRepublishDestinationUpdate.exe .
docker compose up -d
./jetstreamRepublishDestinationUpdate.exe 2>&1 | tee run-output.txt
docker compose down
```

## 시나리오

### Phase A — bridge.v1.> 로 republish 중인 상태

1. `SRC` stream을 `RePublish -> bridge.v1.>` 로 생성
2. `TGT_V1`, `TGT_V2` stream 생성
3. `source.events.alpha` 로 `evt-001 ~ evt-003` 발행
4. 어느 target stream으로 들어갔는지 확인

### Phase B — 운영 중 republish destination 변경

1. `CreateOrUpdateStream()` 으로 `SRC.RePublish.Destination` 을 `bridge.v2.>` 로 변경
2. `source.events.beta` 로 `evt-004 ~ evt-005` 발행
3. 변경 이후 메시지가 `TGT_V2` 로만 들어가는지 확인
4. 기존 `TGT_V1` 메시지가 소급 이동하지 않는지 확인

## 실행 결과

### Phase A 결과

| 확인 항목 | 실제 결과 |
|---|---|
| source stream 상태 | `msgs=3 first_seq=1 last_seq=3 republish={src=> dest=bridge.v1.>}` |
| TGT_V1 상태 | `msgs=3 first_seq=1 last_seq=3` |
| TGT_V2 상태 | `msgs=0 first_seq=0 last_seq=0` |
| TGT_V1 수신 메시지 | `evt-001`, `evt-002`, `evt-003` |
| TGT_V2 수신 메시지 | 없음 |

핵심 로그:

```text
[TGT_V1 after evt-001~003] stream=TGT_V1 msgs=3 first_seq=1 last_seq=3 republish=<none>
[TGT_V2 after evt-001~003] stream=TGT_V2 msgs=0 first_seq=0 last_seq=0 republish=<none>
consumer=v1-consumer-a deliveries=[evt-001(seq=1,subject=bridge.v1.source.events.alpha,payload=payload-001), evt-002(seq=2,subject=bridge.v1.source.events.alpha,payload=payload-002), evt-003(seq=3,subject=bridge.v1.source.events.alpha,payload=payload-003)]
```

결론:
- 초기 설정대로 신규 메시지는 `bridge.v1.>` 쪽으로 정상 republish 되었다.

### Phase B 결과

| 확인 항목 | 실제 결과 |
|---|---|
| source stream 업데이트 직후 | `msgs=3 first_seq=1 last_seq=3 republish={src=> dest=bridge.v2.>}` |
| 추가 발행 후 source 상태 | `msgs=5 first_seq=1 last_seq=5` |
| 추가 발행 후 TGT_V1 상태 | `msgs=3 first_seq=1 last_seq=3` |
| 추가 발행 후 TGT_V2 상태 | `msgs=2 first_seq=1 last_seq=2` |
| old destination 신규 수신 | 없음 |
| new destination 신규 수신 | `evt-004`, `evt-005` |

핵심 로그:

```text
stream 준비: SRC subjects=[source.events.>] republish={src=> dest=bridge.v2.>}
[source after republish destination update] stream=SRC msgs=3 first_seq=1 last_seq=3 republish={src=> dest=bridge.v2.>}
[TGT_V1 after evt-004~005] stream=TGT_V1 msgs=3 first_seq=1 last_seq=3 republish=<none>
[TGT_V2 after evt-004~005] stream=TGT_V2 msgs=2 first_seq=1 last_seq=2 republish=<none>
consumer=v1-consumer-b deliveries=[] (old destination 으로 신규 republish 없음 확인)
consumer=v2-consumer-b deliveries=[evt-004(seq=1,subject=bridge.v2.source.events.beta,payload=payload-004), evt-005(seq=2,subject=bridge.v2.source.events.beta,payload=payload-005)]
```

결론:
- 운영 중 `CreateOrUpdateStream()` 으로 republish destination 변경이 실제로 반영되었다.
- 변경 이후 신규 메시지는 old destination(`bridge.v1.>`) 이 아니라 new destination(`bridge.v2.>`) 으로만 들어갔다.
- 기존에 `TGT_V1` 에 있던 3개 메시지는 그대로 남았고, `TGT_V2` 로 소급 이동하지 않았다.

## 최종 결론

1. **가능하다.** 운영 중인 JetStream stream의 `RePublish.Destination` 은 `CreateOrUpdateStream()` 으로 변경 가능했다.
2. **변경은 신규 유입 메시지부터 적용된다.** destination 변경 후 들어온 메시지는 새 subject/새 stream 쪽으로만 republish 되었다.
3. **소급 이동은 없다.** 변경 전 old destination 으로 이미 republish 된 메시지는 그대로 남고, 새 destination 으로 다시 복제되지 않았다.

## 운영 관점 해석

tenant stream이 현재 old subject로 republish 중이라면,
운영 중에 그 destination 을 새 subject로 바꾸는 전략 자체는 성립한다.

다만 해석은 이렇게 해야 한다.

- 변경 시점 이후 신규 메시지: 새 destination 으로 감
- 변경 시점 이전에 old destination 으로 이미 간 메시지: old 쪽에 그대로 남음

즉, 이건 **republish destination cutover** 이지, 예전 republished 메시지까지 새 경로로 재배치하는 migration 은 아니다.

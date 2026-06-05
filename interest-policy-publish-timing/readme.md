# InterestPolicy 발행 시점 검증 PoC

## 검증 목적

> "InterestPolicy에서 발행 시점에 컨슈머가 없으면 메시지가 버려진다"

이 가설을 NATS **2.12.2** 기준으로 6개 Phase를 통해 실증 검증한다.

---

## 환경

| 항목 | 값 |
|------|-----|
| NATS Server | 2.12.2 |
| nats.go 클라이언트 | v1.52.0 |
| Retention Policy | InterestPolicy |
| Storage | MemoryStorage |

---

## 검증 시나리오 및 결과

### Phase 1 — 컨슈머 없이 발행

| | |
|---|---|
| **시나리오** | InterestPolicy 스트림에 컨슈머가 전혀 없는 상태에서 메시지 3개 발행 |
| **기대값** | `Stream.Msgs = 0` (관심 있는 컨슈머가 없으므로 즉시 버려짐) |
| **실제값** | `Stream.Msgs = 0` |
| **결론** | ✓ **가설 확인.** 발행 시점에 컨슈머가 없으면 메시지는 스트림에 저장되지 않고 버려진다. |

---

### Phase 2 — Durable 컨슈머 생성 후 발행

| | |
|---|---|
| **시나리오** | Durable1 컨슈머 생성 → 메시지 3개 발행 |
| **기대값** | `Stream.Msgs = 3`, Durable1 `NumPending = 3` |
| **실제값** | `Stream.Msgs = 3`, `NumPending = 3` |
| **결론** | ✓ 컨슈머가 존재하면 메시지가 스트림에 보존된다. |

---

### Phase 3 — 모든 컨슈머 ack 완료 후 상태

| | |
|---|---|
| **시나리오** | Phase 2 메시지 3개를 Durable1이 모두 ack |
| **기대값** | `Stream.Msgs = 0` |
| **실제값** | `Stream.Msgs = 0` |
| **결론** | ✓ 관심 있는 모든 컨슈머가 ack하면 메시지가 즉시 삭제된다. |

---

### Phase 4 — 2개 Durable 중 1개만 ack

| | |
|---|---|
| **시나리오** | Durable1 + Durable2 모두 존재 → 메시지 3개 발행 → Durable1만 ack |
| **기대값** | Durable1 ack 후에도 `Stream.Msgs = 3` (Durable2 pending 때문) |
| **실제값** | Durable1 ack 후 `Stream.Msgs = 3`, Durable2 ack 후 `Stream.Msgs = 0` |
| **결론** | ✓ **AND 조건.** 메시지는 관심 있는 컨슈머 **전원**이 ack해야 삭제된다. |

---

### Phase 5 — 발행 후 컨슈머 생성 (DeliverAll)

| | |
|---|---|
| **시나리오** | 컨슈머 없는 상태에서 메시지 3개 발행 → 이후 Durable3(DeliverAll) 생성 |
| **기대값** | Durable3 수신 메시지 수 = 0 |
| **실제값** | 수신 메시지 수 = 0 |
| **결론** | ✓ 발행 시점에 컨슈머가 없어서 버려진 메시지는 **DeliverAll로도 복구 불가**. 나중에 컨슈머가 생겨도 소용없다. |

---

### Phase 6 — 컨슈머 생성 → 발행 → 컨슈머 삭제 (미ack)

| | |
|---|---|
| **시나리오** | Durable4 생성 → 메시지 3개 발행 → Durable4 강제 삭제 (ack 없이) |
| **기대값** | `Stream.Msgs = 0` (관심 컨슈머가 삭제되면 pending도 해제) |
| **실제값** | 삭제 후 200ms, 1s, 3s 모두 `Stream.Msgs = 3` (메시지 잔류) |
| **결론** | ⚠️ **예상 밖 동작 발견.** Durable consumer를 강제 삭제해도 그 pending 메시지는 즉시 정리되지 않는다. |

#### Phase 6 부연 설명

이 동작은 버그가 아니라 NATS의 의도된 설계다:

- **Ephemeral** consumer는 비활성(`InactiveThreshold`) 초과 시 NATS가 자동 삭제하고, 해당 pending 메시지도 정리한다.
- **Durable** consumer를 API로 강제 삭제할 때는 메시지가 즉시 정리되지 않는다. 스트림의 `MaxAge`, `MaxMsgs`, `MaxBytes` 같은 한도에 의해 나중에 정리된다.
- 운영 시 Durable consumer를 삭제할 예정이라면 **삭제 전 `PurgeConsumer`를 호출하거나**, 스트림에 `MaxAge` / `MaxMsgs` 한도를 반드시 설정해야 메시지 누적을 방지할 수 있다.

---

## 핵심 결론

```
InterestPolicy 메시지 보존의 선결 조건 = 발행 시점의 컨슈머 존재
```

| 상황 | 메시지 보존 여부 |
|------|----------------|
| 발행 시 컨슈머 없음 | ✗ 버려짐 |
| 발행 시 컨슈머 있음 | ✓ 보존됨 |
| 모든 컨슈머 ack 완료 | ✗ 삭제됨 |
| 일부 컨슈머만 ack | ✓ 보존됨 (미ack 컨슈머가 남아있는 한) |
| 발행 후 컨슈머 생성(DeliverAll) | ✗ 복구 불가 |
| Durable 강제 삭제(미ack) | ⚠️ 즉시 삭제 안 됨 (한도 도달 시까지 잔류) |

---

## 실행 방법

```bash
# 1. NATS 서버 기동
docker compose up -d

# 2. PoC 실행
go run main.go

# 3. 종료
docker compose down
```

---

## 파일 구성

```
interest-policy-publish-timing/
├── main.go               # 검증 코드 (Phase 1~6)
├── docker-compose.yaml   # NATS 2.12.2 서버
├── nats.conf             # JetStream 활성화 설정
├── go.mod / go.sum
└── readme.md
```

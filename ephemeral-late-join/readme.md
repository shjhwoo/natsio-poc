# Ephemeral Late-Join + InterestPolicy PoC

## 검증 목적

JetStream `InterestPolicy` 스트림에서 **ephemeral consumer가 늦게 join하는 상황**을 세 가지 시나리오로 검증한다.
주요 질문: "durable이 이미 ack한 메세지를, 나중에 생긴 ephemeral이 받을 수 있는가?"

배경: WebSocket 클라이언트처럼 ephemeral consumer를 쓰는 경우, 연결이 끊어지면 ephemeral이 삭제되고 재연결 시 새 ephemeral이 생긴다. 이때 연결이 끊긴 사이에 발행된 메세지를 복구할 수 있는가?

---

## 구성

| 항목 | 값 |
|------|-----|
| Stream 이름 | `ephemeralTestStream` |
| Subject | `starfruit.ephemeral.test` |
| Retention | `InterestPolicy` |
| Storage | `MemoryStorage` |
| MaxAge | 2분 |
| Durable | `durableConsumer` (AckExplicit, DeliverAll) |
| Ephemeral | InactiveThreshold=5s, AckExplicit, DeliverAll |

---

## 시나리오

### Phase 1 — Durable만 있을 때 ack → 늦게 생긴 Ephemeral

1. durable consumer만 존재하는 상태에서 3개 메세지 발행
2. durable이 3개 모두 ack
3. **이후** ephemeral consumer 생성 (DeliverAll)
4. ephemeral이 메세지를 받을 수 있는가?

### Phase 2 — Ephemeral 비활성 삭제 → Durable ack → 새 Ephemeral Late Join

1. ephemeral + durable 모두 존재하는 상태에서 3개 메세지 발행
2. ephemeral이 구독 없이 `InactiveThreshold`(5s)를 초과 → NATS가 자동 삭제
3. durable이 3개 모두 ack
4. 새 ephemeral 생성 (DeliverAll) → 메세지를 받을 수 있는가?

### Phase 3 (보너스) — Ephemeral 비활성 삭제 / Durable 미ack 상태에서 새 Ephemeral Join

1. ephemeral + durable 모두 존재하는 상태에서 3개 메세지 발행
2. ephemeral이 `InactiveThreshold`(5s) 초과 → NATS가 자동 삭제
3. durable은 **ack하지 않음** (메세지가 스트림에 잔존)
4. 새 ephemeral 생성 (DeliverAll) → 메세지를 받을 수 있는가?

---

## 실행 방법

```bash
# NATS 서버 시작
docker compose up -d

# PoC 실행
go run .

# 정리
docker compose down -v
```

---

## 실행 결과 (NATS 2.x, nats.go v1.43.0)

### Phase 1 — ✓ 예상대로 0개 수신

```
[durable] 수신 및 ack: P1-Event-001
[durable] 수신 및 ack: P1-Event-002
[durable] 수신 및 ack: P1-Event-003
Stream.Msgs = 1   ← (주의: 아래 비고 참조)
[Phase 1 결과] Ephemeral이 받은 메세지 수: 0
[Phase 1 결론] ✓ durable이 ack한 메세지는 이미 스트림에서 삭제됨. 나중에 생긴 ephemeral은 받을 수 없다.
```

- InterestPolicy에서 durable이 ack하면 메세지가 스트림에서 삭제된다.
- 이후 생성된 ephemeral은 DeliverAll 정책이어도 이미 사라진 메세지를 받을 수 없다.

> **비고: `Stream.Msgs = 1`인 이유**
> durable이 3개를 ack한 직후 `Stream.Msgs`가 0이 아니라 1로 찍힌다.
> ack 처리와 실제 메세지 삭제 사이에 미세한 비동기 지연이 있어서 생기는 타이밍 아티팩트다.
> 직후 ephemeral이 fetch를 시도했을 때 0개가 수신된 것을 보면, 그 사이에 삭제가 완료됐음을 알 수 있다.

---

### Phase 2 — ✓ 예상대로 0개 수신

```
>> 발행 직후 Stream 상태:
│  Stream.Msgs = 3
│  [durable/durableConsumer]  NumPending=3, NumAckPending=0
│  [ephemeral/UKNAVyrD]       NumPending=3, NumAckPending=0

>> Ephemeral consumer UKNAVyrD 삭제 확인됨 (consumer not found)
>> Ephemeral2 삭제 후 Stream 상태:
│  Stream.Msgs = 3   ← ephemeral 삭제됐어도 durable ack 전이라 메세지 잔존
│  [durable/durableConsumer]  NumPending=3, NumAckPending=0

>> Durable ack 완료 후 Stream 상태:
│  Stream.Msgs = 1   ← (타이밍 아티팩트, 곧 0으로 수렴)
[Phase 2 결과] Ephemeral3이 받은 메세지 수: 0
```

- ephemeral이 구독 없이 `InactiveThreshold`(5s)를 넘으면 NATS 서버가 자동 삭제한다.
- ephemeral이 삭제돼도 **durable이 ack하기 전까지는 메세지가 스트림에 남아 있다.**
- durable까지 ack하고 나면 InterestPolicy에 의해 메세지 완전 삭제 → 새 ephemeral은 0개 수신.

---

### Phase 3 — ✓ 예상대로 3개 수신 성공

```
>> 발행 직후 Stream 상태:
│  Stream.Msgs = 3
│  [durable/durableConsumer]  NumPending=3, NumAckPending=0
│  [ephemeral/7C3c9nfX]       NumPending=3, NumAckPending=0

>> Ephemeral consumer 7C3c9nfX 삭제 확인됨
>> Ephemeral4 삭제 후 Stream 상태 (durable 미ack):
│  Stream.Msgs = 3   ← durable이 ack 안 했으므로 메세지 잔존
│  [durable/durableConsumer]  NumPending=3, NumAckPending=0

[Phase 3 결과] Ephemeral5이 받은 메세지 수: 3
[Phase 3 결론] ✓ Durable이 아직 ack 안 했으므로 메세지가 스트림에 남아 있었음.
               DeliverAll 정책으로 새 Ephemeral이 해당 메세지를 수신 가능.
     수신: P3-Event-001
     수신: P3-Event-002
     수신: P3-Event-003
```

- ephemeral이 비활성 삭제돼도 **durable이 아직 ack하지 않은 메세지는 스트림에 남는다.**
- 새 ephemeral(DeliverAll)을 생성하면 해당 메세지를 정상 수신할 수 있다.

---

## 결론

| 시나리오 | 새 Ephemeral 수신 가능? | 이유 |
|----------|------------------------|------|
| Phase 1: durable ack 완료 후 ephemeral join | **불가** | InterestPolicy: 모든 consumer가 ack하면 메세지 삭제 |
| Phase 2: ephemeral 비활성 삭제 + durable ack 완료 후 새 ephemeral join | **불가** | 동일: 모든 활성 consumer ack → 메세지 삭제 |
| Phase 3: ephemeral 비활성 삭제 / durable 미ack 상태에서 새 ephemeral join | **가능** | durable 미ack → 메세지 잔존 → DeliverAll로 수신 |

> **실무 시사점 (WebSocket 재연결 등)**
>
> ephemeral consumer 기반 클라이언트(예: WebSocket)가 연결이 끊어진 사이에 발행된 메세지를 복구하려면:
> - durable consumer가 별도로 존재하고 아직 ack하지 않은 상태여야 한다.
> - 즉, ephemeral이 끊긴 동안 메세지를 보존하려면 **durable이 "보관자" 역할을 담당**해야 하고, 재연결한 클라이언트가 메세지를 모두 받아 처리한 뒤 durable을 ack하는 흐름이 필요하다.
> - 만약 durable이 먼저 ack해버리면, 이후 어떤 ephemeral도 해당 메세지를 복구할 수 없다.

# JetStream 스트림 MaxAge 자동 삭제 PoC

## 검증 목적

`InterestPolicy` 스트림은 "구독 중인 모든 durable consumer가 ack할 때까지" 메시지를 보존한다. 따라서 모든 소비 서비스가 다운된 상태에서는 ack를 받지 못해 메시지가 무한정 적체된다.

이 PoC는 **스트림 단위 공통 TTL** (`StreamConfig.MaxAge`) 이 적체 문제를 해결하는지 검증한다.

- `MaxAge`는 스트림 전체에 일률 적용되는 보관 시간 상한
- **publisher가 헤더를 깜빡해도** 모든 메시지에 강제 적용 — 메시지별 `Nats-TTL` 방식과의 결정적 차이
- TTL이 지나면 서버가 소비자 상태와 무관하게 자동 삭제

---

## 구성

### Stream

| 항목 | 값 |
|------|-----|
| 이름 | `events` |
| Subjects | `events.>` |
| Retention | `InterestPolicy` |
| Storage | `FileStorage` |
| `MaxAge` | `5s` |

### Consumer

| Consumer | FilterSubjects |
|----------|----------------|
| `ServiceA` (durable) | `events.payment.>` |

### 상수

| 이름 | 값 |
|------|-----|
| `streamMaxAge` | `5s` (스트림 단위 공통 TTL) |

발행 코드는 일반 `nc.Publish(subject, data)` — **TTL 헤더를 일부러 넣지 않는다**. publisher가 협조하지 않아도 동작한다는 점을 보이기 위함.

---

## 테스트 시나리오 및 기대 결과

### Phase 1 — 정상 상태

**시나리오**: ServiceA 가동 → 3개 발행 (헤더 없음)

**기대**: ServiceA가 ack → InterestPolicy 만족 → MaxAge 도달 전에 즉시 삭제
- `Stream.Msgs = 0`
- ServiceA 처리 = 3

---

### Phase 2 — 서비스 다운 → MaxAge 도달

**시나리오**: ServiceA 정지 (durable은 서버에 남음) → 3개 발행 → 약 8초 대기

**기대**
- 발행 직후: `Stream.Msgs = 3`, `ServiceA NumPending = 3` (적체)
- MaxAge 5s 경과 후: 서버가 소비자 없이도 일괄 정리 → `Stream.Msgs = 0`
- → **publisher가 TTL 헤더를 안 붙여도, 스트림 설정만으로 자동 정리됨**을 확인

---

### Phase 3 — 서비스 복구

**시나리오**: ServiceA 재시작 → 3개 신규 발행

**기대**: 정상 흐름 재개 → 즉시 ack 후 삭제, `Stream.Msgs = 0`

---

## MaxAge vs `Nats-TTL` 헤더

| 항목 | `MaxAge` (스트림 설정) | `AllowMsgTTL` + `Nats-TTL` 헤더 |
|------|------------------------|----------------------------------|
| 적용 범위 | 스트림의 모든 메시지 | 메시지마다 다르게 |
| publisher 책임 | 없음 (강제 적용) | 헤더 누락 시 무한 보존 |
| 차등 TTL | 불가 | 가능 |
| 운영 안전망 | ✅ | ❌ |

운영 권장: `MaxAge`를 안전망으로 두고, 차등 TTL이 필요한 메시지에 한해 `Nats-TTL`로 단축. (단축만 가능, MaxAge 초과 불가)

---

## PoC 통과 기준

1. Phase 2에서 ServiceA가 다운된 상태였음에도 `streamMaxAge` 경과 후 `Stream.Msgs = 0`
2. 그 사이 ServiceA durable의 `NumPending` 도 자연스럽게 0으로 감소
3. **모든 발행이 헤더 없이 이루어졌음**에도 정리되는 것 — `MaxAge` 단독으로 충분함을 입증
4. Phase 3에서 신규 메시지가 정상 처리되어 흐름이 망가지지 않음

---

## 실행 방법

```bash
# NATS 서버 시작 (MaxAge는 기본 기능이라 모든 NATS 2.x에서 동작)
docker compose up -d

# PoC 실행
go run .
```

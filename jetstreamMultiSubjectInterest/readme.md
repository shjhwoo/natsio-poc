# JetStream Multi-Subject InterestPolicy PoC

## 검증 목적

JetStream의 `InterestPolicy`는 **스트림에 등록된 모든 consumer가 ack하면 메시지를 삭제**한다.

그런데 consumer마다 `FilterSubjects`가 다를 때 — 즉, 특정 subject를 구독하지 않는 consumer가 있을 때 — 그 consumer의 존재가 해당 subject 메시지의 삭제 조건에 영향을 주는지 확인한다.

**핵심 질문**: subject2를 구독하지 않는 ConsumerC가 내려가 있을 때, subject2 메시지는 삭제되는가?

---

## 구성

### Stream

| 항목 | 값 |
|------|-----|
| 이름 | `multiSubjectSink` |
| Subjects | `test.subject1`, `test.subject2`, `test.subject3` |
| Retention | `InterestPolicy` |
| Storage | `FileStorage` |

### Consumer 구독 관계

| Consumer | FilterSubjects | 의미 |
|----------|---------------|------|
| ConsumerA (serviceA) | subject1, subject2 | subject3에는 관심 없음 |
| ConsumerB (serviceB) | subject1, subject2 | subject3에는 관심 없음 |
| ConsumerC (serviceC) | subject1, subject3 | subject2에는 관심 없음 |

```
             subject1  subject2  subject3
serviceA  →    ✅        ✅        ✗
serviceB  →    ✅        ✅        ✗
serviceC  →    ✅        ✗        ✅
```

---

## 테스트 시나리오 및 결과

### Phase 1 — 전체 서비스 가동, 모든 subject에 발행

**시나리오**: A, B, C 모두 가동 → subject1·2·3에 각 3개 발행

**예상**: 각 consumer는 자신의 FilterSubjects에 해당하는 메시지만 수신·ack → 전부 삭제

**결과**:
```
Stream.Msgs = 0
ConsumerA: NumPending=0, NumAckPending=0
ConsumerB: NumPending=0, NumAckPending=0
ConsumerC: NumPending=0, NumAckPending=0
```

수신 로그를 보면 subject1은 A·B·C 모두, subject2는 A·B만, subject3은 C만 수신한 것을 확인할 수 있다.

---

### Phase 2 — serviceC 중단 후 subject2 발행

**시나리오**: C를 정지 → subject2에 3개 발행 → A·B만 ack

**예상**: ConsumerC의 FilterSubjects에 subject2가 없으므로, C의 존재는 subject2 메시지 삭제 조건에 무관 → **Stream.Msgs = 0**

**결과**:
```
Stream.Msgs = 0
ConsumerA: NumPending=0, NumAckPending=0
ConsumerB: NumPending=0, NumAckPending=0
ConsumerC: NumPending=0, NumAckPending=0  ← C가 꺼져도 subject2는 즉시 삭제
```

**해석**: ConsumerC는 subject2에 대한 interest가 없기 때문에, 서버는 처음부터 "이 메시지를 기다리는 consumer는 A·B뿐"이라고 판단한다. C의 상태와 무관하게 A·B ack 즉시 삭제된다.

---

### Phase 3 — serviceC 중단 상태에서 subject1 발행

**시나리오**: C 정지 유지 → subject1에 3개 발행 → A·B만 ack

**예상**: subject1은 C도 구독 → C의 durable consumer가 서버에 남아있어 메시지 보존 → **Stream.Msgs = 3**

**결과**:
```
Stream.Msgs = 3
ConsumerA: NumPending=0, NumAckPending=0
ConsumerB: NumPending=0, NumAckPending=0
ConsumerC: NumPending=3, NumAckPending=0  ← C가 아직 읽지 않음
```

**해석**: subject1은 ConsumerC의 FilterSubjects에 포함되므로, A·B가 ack해도 C가 ack하기 전까지 메시지가 보존된다. Phase 2와 대비되는 결과로, InterestPolicy의 삭제 조건이 subject 단위로 적용됨을 보여준다.

---

### Phase 4 — serviceC 복구

**시나리오**: C 재시작 → 밀린 subject1 메시지 처리

**예상**: ConsumerC NumPending 소진 → Stream.Msgs = 0

**결과**:
```
Stream.Msgs = 0
ConsumerA: NumPending=0, NumAckPending=0
ConsumerB: NumPending=0, NumAckPending=0
ConsumerC: NumPending=0, NumAckPending=0
```

C가 재연결하자마자 밀린 3개를 처리하고, 이후 즉시 삭제되었다.

---

### Phase 5 — subject3 발행 (C만 구독)

**시나리오**: 전체 가동 → subject3에 3개 발행 → C만 ack

**예상**: A·B의 FilterSubjects에 subject3 없음 → C만 ack해도 즉시 삭제 → **Stream.Msgs = 0**

**결과**:
```
Stream.Msgs = 0
ConsumerA: NumPending=0, NumAckPending=0
ConsumerB: NumPending=0, NumAckPending=0
ConsumerC: NumPending=0, NumAckPending=0
```

subject3은 ConsumerA·B의 interest 대상이 아니므로, C 단독 ack만으로 삭제된다.

---

## 결론

> **InterestPolicy의 삭제 조건은 "해당 메시지의 subject를 FilterSubjects에 포함한 모든 durable consumer가 ack했는가"이다.**

consumer가 존재하더라도 그 consumer의 FilterSubjects에 해당 subject가 포함되지 않으면, 그 consumer는 삭제 조건 계산에서 제외된다.

Phase 2(subject2, C 미구독) vs Phase 3(subject1, C 구독)의 대비가 이를 가장 명확히 보여준다:
- 같은 상황(C 중단)에서 subject2 메시지는 즉시 삭제, subject1 메시지는 C 복구 전까지 보존

이 동작 덕분에 서비스마다 관심 있는 이벤트 타입이 다른 fanout 구조에서도 InterestPolicy가 정확하게 적용된다.

---

## 실행 방법

```bash
# NATS 서버 시작
docker compose up -d

# 테스트 실행
go run .
```

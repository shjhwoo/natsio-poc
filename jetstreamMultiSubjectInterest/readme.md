# JetStream InterestPolicy × Competing Consumers PoC

## 검증 목적

JetStream에서 **두 가지 동작이 같은 스트림 위에서 동시에 성립**하는지 검증한다.

1. **InterestPolicy (subject별 삭제 조건)**
   `InterestPolicy`는 "해당 메시지의 subject를 `FilterSubjects`에 포함한 모든 durable consumer가 ack했을 때" 메시지를 삭제한다. 서비스마다 관심 있는 subject 집합이 다른 fanout 구조에서 이 조건이 subject 단위로 정확히 적용되는지 확인한다.

2. **Competing Consumers (durable 내부 인스턴스 분산)**
   하나의 durable consumer에 **여러 인스턴스(클라이언트)**가 `Consume()`으로 붙으면 서버가 해당 인스턴스들에게 메시지를 **중복 없이 분산**해 전달한다. 인스턴스 장애 시 살아있는 인스턴스로 failover가 일어나는지, 복구 시 다시 분산 상태로 돌아오는지 확인한다.

두 동작이 **서로 간섭 없이** 함께 성립해야 한다.
- 인스턴스 레벨 분산이 일어나도 (= 같은 durable의 어떤 인스턴스가 ack해도) durable 전체 ack로 집계되어 InterestPolicy의 삭제 조건이 정상 동작
- 인스턴스 중 일부가 죽어도 같은 durable의 다른 인스턴스가 ack를 처리하는 한 InterestPolicy 관점에서는 영향이 없어야 함
- 같은 streamSeq를 같은 durable 내 두 인스턴스가 동시에 받지 않아야 함 (중복 처리 0)

---

## 구성

### Stream

| 항목 | 값 |
|------|-----|
| 이름 | `multiSubjectSink` |
| Subjects | `test.subject1`, `test.subject2`, `test.subject3` |
| Retention | `InterestPolicy` |
| Storage | `FileStorage` |

### Consumer 구독 관계 (durable 단위)

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

### 인스턴스 구성

`instanceCount = 2` — 각 durable에 인스턴스 2개가 같이 붙는다.

```
                durable             instances
serviceA  →  ConsumerA  →  [ inst1, inst2 ]   ← 같은 durable을 공유
serviceB  →  ConsumerB  →  [ inst1, inst2 ]
serviceC  →  ConsumerC  →  [ inst1, inst2 ]
```

### 중복 감지 방법

`ServiceHandle.processed`는 `streamSeq → instanceID` 맵이다. 메시지 처리 시 `recordAndCheck(seq, instanceID)`가 호출되어 같은 stream sequence가 이미 다른 인스턴스에 기록돼 있으면 **중복 처리**로 카운트한다. 동일 durable 인스턴스 간 분산이 깨질 때에만 중복이 발생한다.

---

## 테스트 시나리오 및 기대 결과

각 subject에 `msgsPerSubject = 10`개씩 발행한다.

### Phase 1 — 정상 상태: 모든 서비스 × 인스턴스 2개 가동

**시나리오**: 6개 인스턴스 모두 가동, subject1·2·3에 각 10개씩 발행

**검증 포인트**

- *InterestPolicy*:
  - subject1 → A·B·C 모두 ack → 삭제
  - subject2 → A·B만 interest 보유, 둘 다 ack → 삭제 (C는 애초에 후보가 아님)
  - subject3 → C만 interest 보유, ack → 삭제
  - 최종 `Stream.Msgs = 0`
- *Competing Consumers*:
  - 각 서비스의 inst1·inst2 처리 건수가 **둘 다 0이 아닌 값** (분산)
  - 모든 서비스에서 `중복 : 0건`

---

### Phase 2 — serviceA inst1 장애 → inst2 단독 처리

**시나리오**: `serviceA-inst1` 정지 (durable `ConsumerA` 자체는 inst2가 잡고 있어 살아있음) → subject1·2에 각 10개 발행

**검증 포인트**

- *InterestPolicy*:
  - durable `ConsumerA`는 inst2를 통해 계속 ack 가능 → A·B·C(또는 A·B) 모든 durable이 ack 가능
  - subject1·2 모두 정상 삭제, `Stream.Msgs = 0`
  - → **인스턴스 일부 장애는 InterestPolicy의 삭제 조건에 영향을 주지 않는다**는 것을 확인
- *Competing Consumers*:
  - 정지된 `serviceA-inst1`의 카운트는 그대로 멈춤
  - `serviceA-inst2`가 추가 메시지를 **혼자 모두** 처리 (failover)
  - serviceB·C는 여전히 inst1·inst2가 분담
  - 누적 중복 0건 유지

---

### Phase 3 — serviceA inst1 복구

**시나리오**: 같은 durable `ConsumerA`에 `serviceA-inst1-recovered`를 attach → subject1·2에 각 10개 발행

**검증 포인트**

- *InterestPolicy*: 변함없이 정상 삭제, `Stream.Msgs = 0`
- *Competing Consumers*:
  - 복구된 inst1과 inst2가 다시 분산 처리 재개 (두 인스턴스 모두 카운트 증가)
  - 누적 중복 0건 유지

---

## 결론 출력

마지막에 모든 서비스의 중복 건수를 합산해 PASS/FAIL을 출력한다.

```
========== 최종 검증 결과 ==========
[PASS] serviceA: 중복 처리 없음
[PASS] serviceB: 중복 처리 없음
[PASS] serviceC: 중복 처리 없음
>> 모든 서비스에서 Competing Consumers 정상 동작 확인
```

이와 함께 각 Phase 종료 시점의 `Stream.Msgs`가 0인지 확인하면 InterestPolicy도 같이 정상 동작했음을 입증할 수 있다.

---

## 실행 방법

```bash
# NATS 서버 시작
docker compose up -d

# 테스트 실행
go run .
```

실행 시 다음을 모두 만족해야 PoC 통과로 본다.

1. 모든 Phase 종료 시 `Stream.Msgs = 0` (InterestPolicy 정상)
2. 각 Phase에서 같은 durable의 인스턴스 카운트가 **분산** 또는 **failover** 양상으로 나타남 (Competing Consumers 정상)
3. 누적 `중복 : 0건` (분산이 깨지지 않음)

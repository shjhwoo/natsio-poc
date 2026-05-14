# JetStream CreateOrUpdateStream Subjects 변경 PoC

## 검증 목적

운영 중인 JetStream stream의 `Subjects`를 `CreateOrUpdateStream`으로 바꿀 수 있는지, 그리고 그 부수 효과(잔존 메시지, consumer 상태, 신규 publish 거부 여부)를 직접 관찰한다.

---

## 핵심 질문

1. **subject 추가**: 운영중 stream에 새 subject를 더할 수 있는가? 추가된 subject 메시지는 기존 consumer가 자동으로 받는가, 아니면 consumer의 `FilterSubjects`도 따로 갱신해야 하는가?
2. **subject 제거 + 잔존 메시지**: 제거하려는 subject에 메시지가 남아있을 때 update는 성공하는가? 성공한다면 그 메시지들은 어떻게 되는가?
3. **subject 완전 교체**: 기존과 전혀 겹치지 않는 subject 집합으로 바꿀 수 있는가?
4. **stale consumer**: stream에서 사라진 subject를 가리키는 consumer는 어떤 상태가 되는가? 새 subject로 갱신할 수 있는가?

---

## 구성

| 항목 | 값 |
|------|-----|
| Stream 이름 | `subjectUpdateTest` |
| 초기 Subjects | `evt.a`, `evt.b` |
| Retention | `LimitsPolicy` (잔존 메시지 관찰을 위해) |
| Storage | `FileStorage` |
| Consumer | durable=`Worker`, FilterSubjects=초기 stream subjects와 동일 |

`main.go`는 매 실행 시작 시 기존 stream을 `DeleteStream`으로 제거하므로, 깨끗한 상태에서 재현 가능하다.

---

## 시나리오

### Phase 0 — 초기 구성
Subjects=[evt.a, evt.b]로 stream/consumer 생성, 각 subject에 2개씩 발행하여 정상 ack까지 확인.

### Phase 1 — subject 추가
`CreateOrUpdateStream`으로 Subjects를 `[evt.a, evt.b, evt.c]`로 확장.
- 확장 직후 stream config가 갱신되었는지
- 새 subject(`evt.c`)로 발행한 메시지가 stream에 쌓이지만, consumer FilterSubjects가 아직 `[evt.a, evt.b]`인 동안에는 수신되지 않는지
- Consumer를 `[evt.a, evt.b, evt.c]`로 `CreateOrUpdateConsumer`하면 밀린 evt.c 메시지를 흡수하는지

### Phase 2 — subject 제거
1. consumer를 멈추고 `evt.a`에 5개를 발행하여 stream에 잔존시킴
2. 잔존 상태에서 Subjects를 `[evt.b, evt.c]`로 update 시도 → 성공/실패 관찰
3. `Purge(WithPurgeSubject(evt.a))` 후 다시 시도
4. 제거된 `evt.a`로 publish 시 JetStream이 거부하는지 (`no stream matches subject` 류)

### Phase 3 — subject 완전 교체
Subjects를 `[brand.new.x, brand.new.y]`로 전부 갈아치움. 신규 subject에 발행이 정상 수신되는지.

### Phase 4 — stale consumer
이 시점에서 consumer의 `FilterSubjects`는 Phase 1에서 갱신한 `[evt.a, evt.b, evt.c]`인데 stream subjects는 `[brand.new.x, brand.new.y]`다.
- `consumer.Info()`에서 어떻게 보이는지
- 새 subject로 `CreateOrUpdateConsumer`가 성공하는지

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

## 출력 읽는 법

각 Phase 끝 또는 update 직후에 다음과 같은 박스가 찍힌다:

```
┌─ [Phase 1 update 직후]
│  Config.Subjects = [evt.a evt.b evt.c]
│  State.Msgs      = 0   (FirstSeq=0, LastSeq=4)
└──────────────────────────────
```

- `Config.Subjects`: update가 반영됐는지의 진실값
- `State.Msgs`: 잔존 메시지 수
- `FirstSeq` / `LastSeq`: purge/삭제 발생 시 변화를 보기 위함

`>> CreateOrUpdateStream(...) 실패: code=... errCode=... desc=...` 로그가 찍히면 NATS가 거부한 케이스다 — `errCode`/`desc`로 어떤 종류의 거부인지 식별할 수 있다.

---

## 실행 결과 (NATS 2.x, nats.go v1.43.0)

### Phase 1 — subject 추가
- `CreateOrUpdateStream([evt.a, evt.b, evt.c])` **성공**, `Config.Subjects` 즉시 반영
- 새 `evt.c`에 발행한 메시지는 stream에 정상 적재 (`State.Msgs` 4 → 7)
- ⚠️ **기존 `consumer.Consume()`로 돌아가던 ConsumeContext는 evt.c 메시지를 수신하지 못함.** 서버 측 `CreateOrUpdateConsumer`로 FilterSubjects를 갱신해도, 이미 떠 있던 ConsumeContext는 그 변경을 흡수하지 않는다. 받으려면 기존 cc를 Stop → 갱신된 consumer 핸들로 다시 `Consume()` 해야 한다.

### Phase 2 — subject 제거
- (2-b) **`evt.a`에 메시지 5개가 남아 있는 상태에서도 `CreateOrUpdateStream([evt.b, evt.c])` 성공.** `State.Msgs`는 12 그대로 — stream config와 매치되지 않는 메시지가 stream 안에 남는다.
- (2-c) `stream.Purge(WithPurgeSubject(evt.a))`로 7개(`evt.a` 전부) 제거 → `State.Msgs` 12 → 5, `FirstSeq` 1 → 3 (`LastSeq`는 12 유지 → 시퀀스 번호는 재사용되지 않음)
- (2-d) 제거된 `evt.a`로 publish → **`nats: no response from stream`으로 거부**

### Phase 3 — subject 완전 교체
- `[evt.b, evt.c] → [brand.new.x, brand.new.y]` 교체 **성공**
- ⚠️ 이전 `evt.b`·`evt.c` 메시지 5개는 그대로 stream에 남음 — 교체는 신규 publish 라우팅만 바꿀 뿐, 기존 데이터는 보존된다
- 새 subject로 발행 정상 (`State.Msgs` 5 → 9)

### Phase 4 — stale consumer
- 직전까지 consumer FilterSubjects = `[evt.a, evt.b, evt.c]`, stream Subjects = `[brand.new.x, brand.new.y]` — 완전히 mismatch한 상태에서도 `consumer.Info()` 정상 동작 (`NumPending=3`)
- `CreateOrUpdateConsumer`로 새 subject로 갱신 **성공**

---

## 결론

> **운영중 stream의 Subjects는 `CreateOrUpdateStream`으로 동적으로 바꿀 수 있다.** 다만 stream config만 바뀌지, 데이터·consumer·클라이언트 측 상태는 자동으로 따라오지 않는다.

운영에서 subject를 갱신할 때 챙겨야 할 것:

1. **잔존 메시지 정리는 명시적으로**: subject 제거 시 NATS는 그 subject의 잔존 메시지를 자동 삭제하지 않는다. 필요하면 update 전후로 `stream.Purge(WithPurgeSubject(...))` 호출.
2. **Consumer는 별개로 갱신**: stream에 subject를 추가해도 기존 consumer의 `FilterSubjects`는 그대로다. `CreateOrUpdateConsumer`로 갱신해야 함.
3. **클라이언트 ConsumeContext 재시작 필요**: 서버에서 consumer config가 갱신되어도 이미 떠 있던 `Consume()` 컨텍스트는 변경을 따라가지 않는다. Stop 후 새 consumer 핸들로 다시 시작해야 새 subject를 흡수한다.
4. **제거된 subject로의 publish는 안전하게 거부**: `js.Publish`는 `no response from stream` 에러를 돌려준다 — 발행 측에서 적절히 fail-fast 처리 가능.
5. **시퀀스 번호는 재사용 안 됨**: subject 제거/purge로 메시지가 사라져도 `LastSeq`는 유지된다. consumer의 ack 상태 관리에 유리한 특성.

운영 적용 시 권장 순서: `(1) 새 consumer config 준비` → `(2) CreateOrUpdateStream으로 subject 추가` → `(3) 기존 ConsumeContext Stop` → `(4) CreateOrUpdateConsumer로 FilterSubjects 갱신` → `(5) 새 consumer 핸들로 Consume() 재개` → `(필요 시) 제거 대상 subject Purge` → `(6) CreateOrUpdateStream으로 subject 제거`.

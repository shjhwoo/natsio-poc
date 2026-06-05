# JetStream UpdateStream: Sources / Republish 사후 추가 PoC

## 검증 목적

이미 운영 중인 JetStream 스트림(Sources·Republish 설정이 전혀 없는 상태)에 대해
`CreateOrUpdateStream`으로 두 설정을 **사후에 추가**할 수 있는지 검증한다.

---

## 핵심 질문

| Case | 질문 |
|------|------|
| **Case 1** | Sources 없이 생성된 스트림에 나중에 `Sources`를 추가할 수 있는가? 추가 후 소스 스트림의 메시지가 실제로 흡수되는가? |
| **Case 2** | Republish 없이 생성된 스트림에 나중에 `RePublish`를 추가할 수 있는가? 추가 후 발행한 메시지가 목적지 subject로 재발행되는가? |

---

## 구성

### Case 1 — Sources

| 항목 | 값 |
|------|-----|
| 소스 스트림 | `src-stream` / Subjects: `src.events.>` |
| 대상 스트림 | `tgt-stream` / Subjects: `tgt.events.>` (최초 Sources 없음) |
| Storage | `FileStorage` |

### Case 2 — Republish

| 항목 | 값 |
|------|-----|
| 스트림 | `repub-stream` / Subjects: `repub.in.>` (최초 Republish 없음) |
| Republish 목적지 | `repub.out.>` |
| Storage | `FileStorage` |

---

## 시나리오

### Case 1 — Sources 추가

1. **Phase 1-a**: `src-stream` 생성 (Sources의 원본)
2. **Phase 1-b**: `tgt-stream`을 Sources 없이 생성 → 각각 독립적으로 메시지 적재
3. **Phase 1-c**: `tgt-stream`에 `Sources: [{Name: "src-stream"}]` 추가 시도
   - 성공 시: `src-stream`에 신규 메시지를 발행하여 `tgt-stream`에 흡수되는지 확인

### Case 2 — Republish 추가

1. **Phase 2-a**: `repub-stream`을 Republish 없이 생성
2. **Phase 2-b**: Republish 추가 전 메시지 발행 → `repub.out.>` 구독자에게 메시지가 가지 않음을 확인
3. **Phase 2-c**: `RePublish: {Source: ">", Destination: "repub.out.>"}` 추가 시도
4. **Phase 2-d**: 추가 후 메시지 발행 → `repub.out.>` 구독자에게 메시지가 재발행되는지 확인

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

각 단계마다 다음 형식으로 스트림 상태가 출력된다:

```
┌─ [Sources 추가 후 targetStream]
│  Name            = tgt-stream
│  Config.Subjects = [tgt.events.>]
│  Config.Sources  = [src-stream]
│  Config.RePublish= <없음>
│  State.Msgs      = 8  (FirstSeq=1, LastSeq=8)
└──────────────────────────────
```

- `Config.Sources` / `Config.RePublish`: 설정 반영 여부의 진실값
- `State.Msgs`: 메시지 흡수 여부 확인용

`✗` 로 시작하는 로그가 찍히면 NATS가 거부한 케이스이며, `HTTPCode`·`ErrCode`·`desc`로 원인을 식별할 수 있다.

---

## 실행 결과 (NATS 2.x, nats.go v1.43.0)

### Case 1 — Sources 추가

- `CreateOrUpdateStream`에 `Sources: [{Name: "src-stream"}]`을 추가 → **성공**
- `Config.Sources = [src-stream]` 즉시 반영
- ⚠️ **Sources 추가 시점 이전에 소스 스트림에 쌓인 메시지도 소급 흡수된다.**
  - `src-stream`에 이미 3개 적재된 상태였고, 추가 후 신규 3개를 더 발행하자 `tgt-stream`의 `State.Msgs`가 2 → 8로 증가 (소급 3개 + 신규 3개)
  - `StartSeq`를 별도로 지정하지 않으면 소스 스트림의 첫 번째 메시지부터 가져온다

### Case 2 — Republish 추가

- `CreateOrUpdateStream`에 `RePublish: {Source: ">", Destination: "repub.out.>"}`를 추가 → **성공**
- `Config.RePublish = {src=">" dest="repub.out.>"}` 즉시 반영
- 추가 이전에 발행된 메시지는 재발행되지 않음 (당연)
- **추가 이후** 발행한 3개 메시지 모두 `repub.out.repub.in.after-update` subject로 정상 재발행됨

---

## 결론

> **두 설정 모두 운영 중인 스트림에 `CreateOrUpdateStream`으로 사후 추가 가능하다.**

운영 적용 시 주의할 점:

1. **Sources 추가는 소급 적용됨**: `StartSeq`·`StartTime` 등을 명시하지 않으면 소스 스트림의 처음부터 메시지를 가져온다. 필요한 시점부터만 받으려면 `StreamSource.OptStartSeq` 또는 `OptStartTime`을 지정해야 한다.
2. **Republish는 추가 이후 메시지부터만 적용**: 기존에 스트림에 쌓인 메시지는 재발행되지 않는다.
3. **두 설정 모두 제거도 가능**: `CreateOrUpdateStream`에서 해당 필드를 `nil` / 빈 슬라이스로 보내면 설정을 다시 제거할 수 있다.

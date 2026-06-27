# JetStream WorkQueue → Interest 마이그레이션 스터디

운영 중인 JetStream 환경에서 WorkQueue 기반 stream/consumer 구성을 Interest 기반 구성으로 전환할 때, 어떤 방식이 실제로 안전한지 검증한 재현 가능한 스터디입니다.

## 이 저장소를 만든 이유

메시지 기반 시스템에서 stream retention 이나 consumer 동작을 바꾸는 일은 단순 설정 변경으로 끝나지 않습니다.
겉으로는 API 한 번 호출하면 될 것처럼 보여도, 실제 운영 환경에서는 다음에 영향을 줍니다.

- 메시지 보존 방식
- backlog 유지 여부
- consumer 상태 연속성
- 컷오버 타이밍
- 롤백 가능성

이 저장소는 이 문제를 두 단계로 나눠 검증합니다.

1. 기존 WorkQueue 기반 stream 을 in-place 로 업데이트할 수 있는지 확인한다.
2. 그 경로가 막혀 있다면, 병행 이전 / 컷오버 / 롤백 방식이 더 안전한 대안이 되는지 검증한다.

목표는 "더 좋아 보이는 구조"를 주장하는 것이 아니라, 실제로 무엇이 안전한지 실험으로 좁혀가는 것입니다.

---

## 검증한 질문

### Phase 1 — In-place 설정 변경

- `CreateOrUpdateStream()` 으로 `WorkQueuePolicy` 를 `InterestPolicy` 로 직접 바꿀 수 있는가?
- durable consumer 를 업데이트하면 결과가 달라지는가?
- 직접 변경이 막혀 있다면 delete/recreate 는 안전한가?

### Phase 2 — 병행 이전 / 컷오버

- 기존 WorkQueue stream 과 새 Interest stream 을 dual-write 로 병행 운영할 수 있는가?
- 새 Interest consumer 를 늦게 붙여도 이전 backlog 를 bootstrap 할 수 있는가?
- 그렇지 않다면, shadow consumer 를 미리 만들어두는 방식이 점진적 컷오버를 가능하게 하는가?
- 컷오버 이후에도 롤백 경로를 유지할 수 있는가?

---

## 핵심 결론

### 1) In-place retention update 는 불가능했다

테스트한 환경에서는 `CreateOrUpdateStream()` 으로 `WorkQueuePolicy` 를 `InterestPolicy` 로 바꾸려 할 때 아래 에러가 발생했습니다.

```text
err_code=10052
stream configuration update can not change retention policy to/from workqueue
```

이 제약은 durable consumer 존재 여부와 무관했습니다.

### 2) Consumer update 로는 해결되지 않는다

`CreateOrUpdateConsumer()` 호출 자체는 성공할 수 있지만, stream retention 의미론을 바꾸지는 못합니다.
즉 문제의 본질은 consumer 가 아니라 stream retention 변경 자체입니다.

### 3) Delete/recreate 는 라이브 마이그레이션 경로로 안전하지 않다

stream 을 지우고 다시 만들면 새 retention 모드는 적용되지만, 다음 상태가 초기화됩니다.

- stream 상태
- 메시지 backlog
- consumer 상태
- pending / ack 추적 정보

따라서 운영 중 무손실 마이그레이션 경로로 쓰기 어렵습니다.

### 4) 병행 이전은 가능하지만 순서가 중요하다

상대적으로 안전한 방식은 다음과 같습니다.

- 기존 WorkQueue stream 은 그대로 유지한다.
- 새 Interest 기반 stream 을 별도로 만든다.
- dual-write 시작 전에 새 durable shadow consumer 를 먼저 만든다.
- backlog 동작과 신규 처리 경로를 검증하면서 점진적으로 컷오버한다.
- 롤백 가능성을 위해 old 경로를 충분히 오래 유지한다.

### 5) Consumer 를 늦게 붙이는 것은 위험한 가정이다

테스트한 Interest 기반 경로에서는 dual-write 시작 후에 consumer 를 붙였을 때, earlier backlog bootstrap 이 기대한 방식대로 보존되지 않았습니다.

실무적으로 중요한 의미는 이겁니다.

> 핵심 준비 작업은 “밤에 마이그레이션한다”가 아니라, “컷오버 밤이 오기 전에 새 consumer 를 이미 붙여둔다”는 점이다.

---

## 저장소 구조

```text
docs/
  01-problem-statement.md
  02-inplace-update-findings.md
  03-parallel-cutover-findings.md
  04-migration-strategy.md
  05-lessons-learned.md
  06-publication-checklist.md

poc/
  01-inplace-retention-update/
  02-parallel-cutover-and-rollback/

diagrams/
```

---

## PoC 설명

### `poc/01-inplace-retention-update`

이 PoC 는 다음을 검증합니다.

- `WorkQueuePolicy -> InterestPolicy` in-place retention update 가 지원되는지
- consumer update 가 그 제약을 우회하는지
- delete/recreate 가 상태 유실 없이 안전한지

### `poc/02-parallel-cutover-and-rollback`

이 PoC 는 다음을 검증합니다.

- dual-write 기반 병행 이전이 가능한지
- 새 Interest consumer 를 늦게 붙이면 backlog bootstrap 가 실패하는지
- shadow consumer 선생성이 더 안전한 컷오버 검증을 가능하게 하는지
- old 경로를 유지하는 동안 rollback 이 가능한지

---

## 실험 재현 방법

각 PoC 에는 다음이 포함되어 있습니다.

- Go 소스 코드
- Docker Compose 기반 로컬 3-node NATS 클러스터
- 실제 실행 로그(`run-output.txt`)

기본 실행 흐름은 다음과 같습니다.

```bash
go mod tidy
go build
docker compose up -d
./<binary> 2>&1 | tee run-output.txt
docker compose down
```

정확한 실행 범위는 각 PoC 디렉토리의 README 를 참고하면 됩니다.

---

## 이 스터디의 핵심 의미

이 저장소는 단순히 "설정 바꾸기"에 대한 문서가 아니라, 마이그레이션 의사결정을 어떻게 좁혀 갔는지를 기록합니다.

- 가정을 실험으로 검증하기
- API 수준의 가능성과 운영 수준의 안전성을 구분하기
- 실패 경로를 빨리 확인하기
- 실제 플랫폼 동작에 맞춰 컷오버와 롤백 전략을 설계하기

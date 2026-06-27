# 마이그레이션 전략

## 전략 선택

두 개의 PoC 결과를 종합하면, 권장 경로는 **in-place mutation** 이 아니라 **병행 도입 + 점진적 컷오버** 입니다.

## 피해야 할 것

### Direct retention mutation 에 기대지 말 것
테스트한 환경에서는 `WorkQueuePolicy -> InterestPolicy` 변경이 서버 차원에서 거부되었습니다.
다른 환경에서 별도 검증이 없는 한, 이 경로는 사용할 수 없는 경로로 보는 것이 맞습니다.

### 라이브 마이그레이션에 delete/recreate 를 쓰지 말 것
delete/recreate 는 설정은 바꾸지만, 라이브 전환에서 중요한 상태 연속성을 끊어버립니다.
메시지 이력이나 consumer 상태가 중요하지 않은 경우가 아니라면 운영 전환 경로로 보기 어렵습니다.

### 새 Interest consumer 를 너무 늦게 만들지 말 것
PoC 는 consumer 생성 시점이 backlog 가시성에 직접 영향을 준다는 점을 보여줬습니다.
consumer 를 늦게 붙이면 bootstrap 검증 신뢰도가 떨어지고, 컷오버 판단도 불안정해집니다.

## 권장 운영 순서

### Phase 0 — 인벤토리와 준비 상태 확인
마이그레이션 전에 아래를 정리해야 합니다.

- old stream 이름, subject, consumer topology
- 현재 backlog 크기와 pending 상태
- publish 경로를 어떻게 전환할지
- old/new 양쪽에서 어떤 지표를 볼 수 있는지
- rollback 스위치가 무엇인지

### Phase 1 — 새 경로를 먼저 완성한다
라이브 트래픽을 돌리기 전에 새 경로를 먼저 완성합니다.

1. 새 Interest 기반 stream 생성
2. 새 durable consumer 생성
3. 새 consumer 상태 조회/관찰 가능 여부 확인
4. 배포 또는 routing switch 준비

### Phase 2 — overlap 을 조심스럽게 시작한다
새 consumer 가 이미 존재하는 상태에서만 overlap 을 시작합니다.
이 단계에서는:

- 기존 WorkQueue 경로는 그대로 둔다
- 새 경로에 overlap 트래픽을 제한적으로 보낸다
- 새 경로가 정상적으로 수신/처리하는지 확인한다
- backlog, retry, 처리 오류를 관찰한다

### Phase 3 — 점진적으로 컷오버한다
새 경로가 충분히 안정적이면:

- 새 경로를 primary handling path 로 승격한다
- old 경로는 관찰 구간 동안 계속 유지한다
- publish 성공률, consumer lag, retry, 비즈니스 레벨 정확성을 확인한다

### Phase 4 — rollback 을 실제 옵션으로 보존한다
새 경로가 동작하기 시작했다고 해서 바로 마이그레이션 완료로 보면 안 됩니다.
rollback 이 현실적인 옵션으로 남아 있으려면:

- old stream 이 아직 존재해야 하고
- old consumer 가 다시 일을 이어갈 수 있어야 하며
- 운영 스위치를 다시 old 쪽으로 돌릴 수 있어야 합니다

### Phase 5 — old 경로는 늦게 정리한다
아래가 만족된 뒤에만 old 경로를 제거합니다.

- 새 경로가 충분한 기간 안정적이었다
- old backlog 상태를 충분히 이해했고 더 이상 필요하지 않다
- rollback 가치보다 단순화 이득이 더 크다

## 판단 규칙

마이그레이션이 retention 의미론과 consumer 가시성에 영향을 준다면, 플랫폼 동작은 반드시 실험으로 검증해야 합니다.
그 실험 결과 direct mutation 이 막혀 있거나 unsafe 하다고 나오면, 설정 변경을 억지로 밀기보다 staged cutover 로 전략을 바꾸는 것이 맞습니다.

## 한 줄 요약

가장 안전한 경로는 "stream 을 바로 업데이트하는 것"이 아니라, "새 경로를 먼저 만들고, consumer 를 미리 붙여 검증한 뒤, 점진적으로 컷오버하고 old 경로는 늦게 정리하는 것"이었다.

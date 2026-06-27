# 문제 정의

## 목표

운영 중인 JetStream 환경에서 `WorkQueuePolicy` 기반 구성을 `InterestPolicy` 기반 구성으로 안전하게 전환할 수 있는지 검증하고, 직접 변경이 어렵다면 더 안전한 운영 전략이 무엇인지 찾는 것이 목표입니다.

## 왜 이게 단순 설정 변경 문제가 아닌가

처음 보면 retention 이나 consumer 동작 변경은 단순한 configuration update 처럼 보일 수 있습니다.
하지만 실제 운영 환경에서는 다음 위험을 함께 봐야 합니다.

- retention 의미론에 따른 backlog 보존 차이
- consumer 생성 시점에 따른 데이터 가시성 차이
- stream 삭제/재생성이 상태를 끊어버릴 가능성
- cutover 순서에 따른 장애/롤백 위험

그래서 올바른 질문은 단순히 "API 로 바꿀 수 있는가"가 아니라, "운영 중 상태를 해치지 않고 안전하게 바꿀 수 있는가"입니다.

## 스터디 설계

이번 스터디는 두 개의 PoC 로 나누어 진행했습니다.

### PoC 1 — In-place retention update
목적:
- `CreateOrUpdateStream()` 으로 `WorkQueuePolicy -> InterestPolicy` 변경이 가능한지 확인
- durable consumer update 가 결과를 바꾸는지 확인
- delete/recreate 가 운영적으로 감당 가능한 우회책인지 확인

### PoC 2 — 병행 컷오버와 롤백
목적:
- 기존 WorkQueue 경로를 유지한 채 새 Interest 경로를 병행 도입할 수 있는지 확인
- consumer 를 늦게 붙여도 backlog bootstrap 이 가능한지 확인
- shadow consumer 선생성이 점진적 컷오버를 가능하게 하는지 확인
- 컷오버 이후에도 rollback 경로가 살아 있는지 확인

## 평가 기준

어떤 마이그레이션 경로를 수용 가능한 것으로 보려면 최소한 아래를 만족해야 합니다.

- retention 특성 때문에 숨은 메시지 유실이 없어야 한다
- consumer 상태가 예기치 않게 초기화되지 않아야 한다
- 컷오버 순서가 명확하고 재현 가능해야 한다
- 롤백 경로가 문서상으로만이 아니라 실제로 유지되어야 한다

## 관점 변화

이 스터디는 처음에는 "config update 가 가능한가"라는 질문으로 시작했습니다.
하지만 실험을 거치며 본질은 "운영 전략을 어떻게 설계해야 안전한가"라는 질문으로 바뀌었습니다.

이 저장소가 실패 경로 PoC 와 대안 경로 PoC 를 모두 포함하는 이유도 여기에 있습니다.

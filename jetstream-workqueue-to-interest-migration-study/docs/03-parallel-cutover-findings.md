# 병행 컷오버 결과

## 질문

In-place retention migration 이 불가능하다면, 더 안전한 병행 마이그레이션 경로를 사용할 수 있는가?

## 테스트한 마이그레이션 모델

- 기존 WorkQueue stream 은 그대로 유지한다
- 별도의 Interest 기반 stream 을 새로 만든다
- overlap 기간 동안 새 메시지를 양쪽 경로로 보낸다
- 새 consumer 를 언제 만들어야 하는지 검증한다
- 컷오버 이후 rollback 이 가능한지 확인한다

## 주요 시나리오

### 시나리오 A — shadow consumer 를 미리 만든 병행 구성
새 Interest stream 을 만들고, overlap 트래픽이 들어가기 전에 durable shadow consumer 를 먼저 붙인다.

### 시나리오 B — consumer 를 늦게 붙이는 경우
overlap 트래픽이 먼저 시작된 뒤, 그 다음에 새 Interest consumer 를 붙인다.

### 시나리오 C — 점진적 컷오버
backlog 동작을 충분히 관찰한 뒤 새 경로를 primary consumer path 로 승격한다. 이때 old 경로는 그대로 유지한다.

### 시나리오 D — 롤백
컷오버 이후에도 일정 기간 dual-write 를 유지한 상태에서, old 경로로 다시 되돌릴 수 있는지 확인한다.

## 결과

### 1. 병행 마이그레이션 자체는 가능했다
기존 WorkQueue 경로를 건드리지 않고, 새 Interest 경로를 옆에 추가하는 방식은 점진적 이전 모델로 성립했다.
이 방식은 원본 stream 에 대한 unsafe mutation 을 피할 수 있다.

### 2. Consumer 를 늦게 붙이면 bootstrap 가정이 깨진다
Interest 기반 consumer 를 overlap 시작 후에 붙이면, 이전 메시지가 운영자가 기대하는 방식으로 bootstrap 되지 않았다.

즉 다음 가정은 위험하다.

> "새 stream 만 먼저 만들어 두고, consumer 는 컷오버 시간에 붙이면 되겠지"

이 PoC 는 그 순서가 잘못된 순서임을 보여줬다.

### 3. Shadow consumer 선생성이 핵심 준비 작업이다
새 Interest stream 에 durable consumer 를 미리 붙여두면, 실제 컷오버 전에 backlog 와 신규 경로를 관찰할 수 있다.
즉 새 경로를 주 경로로 올리기 전에 먼저 눈으로 확인 가능한 상태가 된다.

### 4. Rollback 은 old 경로를 실제로 보존할 때만 의미가 있다
rollback 이 가능하려면 아래가 유지되어야 한다.

- old WorkQueue stream 이 살아 있어야 한다
- old consumer 가 계속 재개 가능한 상태여야 한다
- 운영 스위치를 old 쪽으로 되돌릴 수 있어야 한다

즉 rollback 은 문서상의 약속이 아니라, old 경로를 실제 옵션으로 남겨둘 때만 성립한다.

## 실무적 결론

상대적으로 안전한 마이그레이션 순서는 다음과 같다.

1. 새 Interest 기반 stream 생성
2. 새 durable shadow consumer 생성
3. overlap / dual-write 시작
4. backlog 및 신규 처리 경로 검증
5. 점진적 컷오버 수행
6. rollback 안전성을 위해 old 경로를 충분히 유지
7. 안정화가 확인된 뒤 old 경로를 늦게 retire

## 왜 이 PoC 가 중요했는가

이 두 번째 PoC 는 단순히 우회책이 있다는 것만 보여준 것이 아니다.
실제로 안전 여부를 가르는 순서 제약을 밝혀냈다.

**consumer 생성 시점은 단순 배포 디테일이 아니라, 마이그레이션 설계의 일부다.**

# In-place Update 결과

## 질문

운영 중인 JetStream stream 을 `WorkQueuePolicy` 에서 `InterestPolicy` 로 in-place 변경할 수 있는가?

## 환경

- NATS Server: 2.12.2
- nats.go: v1.52.0
- topology: 3-node cluster
- storage: FileStorage
- replicas: 3

## 수행한 테스트

### 시나리오 A
- WorkQueue stream 생성
- 메시지 발행
- `CreateOrUpdateStream()` 으로 `InterestPolicy` 변경 시도

### 시나리오 B
- durable consumer 가 붙은 WorkQueue stream 생성
- pending / ack 상태가 존재하는 운영 중 상태를 재현
- 같은 retention 변경 시도

### 시나리오 C
- 기존 stream 삭제
- 새 retention 정책으로 같은 이름 재생성
- 이전 상태가 유지되는지 확인

## 결과

### 1. In-place retention update 는 서버가 명시적으로 거부했다
테스트 환경에서 아래 에러가 반환됐다.

```text
err_code=10052
stream configuration update can not change retention policy to/from workqueue
```

즉 이 경로는 단순히 위험한 정도가 아니라, 서버 차원에서 직접 차단되는 경로였다.

### 2. Durable consumer update 는 본질적 해결책이 아니었다
consumer update API 호출은 성공할 수 있지만, 그것이 stream retention model 을 바꾸지는 않는다.
문제의 병목은 consumer 가 아니라 retention 변경 자체였다.

### 3. Delete/recreate 는 설정은 바꾸지만 운영 연속성을 깨뜨린다
stream 을 지우고 다시 만들면 `InterestPolicy` 로 재구성할 수는 있다.
하지만 다음이 함께 초기화된다.

- stream backlog
- sequence 진행 상태
- consumer 상태
- pending / ack 추적 정보

그래서 delete/recreate 는 상태 연속성이 중요한 라이브 마이그레이션 경로로는 적절하지 않다.

## 실무적 결론

무손실 in-place migration 경로는 확인되지 않았다.
직접 mutation 이 막혀 있고, delete/recreate 가 상태를 잃게 만든다면 다음 단계는 "config API 를 더 우회해 보기"가 아니라 "병행 마이그레이션 경로를 설계하기"가 된다.

## 왜 이 결과가 중요했는가

이 PoC 는 선택지를 좁혀줬다.
즉 다음 세 가지를 명확히 했다.

- direct update 는 불가능하다
- consumer-only update 는 문제 해결과 무관하다
- 강제 재생성은 운영적으로 unsafe 하다

이 결론 덕분에 마이그레이션 논의의 초점이 configuration mutation 에서 staged cutover 로 이동했다.

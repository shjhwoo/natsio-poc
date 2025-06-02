# WS 메세지 버퍼링 데모

### 목적
- 인터넷 환경이 나쁜 곳 ~ 좋은 곳을 번갈아가며 이동하는 모바일 기기 특성을 고려하여 
WS 연결 잦은 순단을 대비, 메세지를 캐싱해두는 로직입니다

### 구성 인프라

- nats
- redis

### 실행방법
```
docker-compose up -d

go run main.go

cd wsclient && go run main.go  <- 클라이언트 코드를 일정 시간 간격으로 켰다 껐다 해보며 메세지들이 빠짐 없이 들어오는지, 또는 fallback-url 메세지가 들어오는지 확인해보세요

```
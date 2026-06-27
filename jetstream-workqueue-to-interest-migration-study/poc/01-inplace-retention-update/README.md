# PoC 01 — In-place Retention Update

이 PoC 는 운영 중인 JetStream stream 을 `WorkQueuePolicy` 에서 `InterestPolicy` 로 in-place 변경할 수 있는지 검증합니다.

## 범위

다음 세 가지를 확인합니다.

1. stream retention 을 직접 업데이트할 수 있는가?
2. durable consumer 를 업데이트하면 결과가 달라지는가?
3. direct update 가 막혀 있다면, delete/recreate 를 마이그레이션 경로로 볼 수 있는가?

## 파일 구성

- `main.go` — 테스트 드라이버
- `docker-compose.yaml`, `n1.conf`, `n2.conf`, `n3.conf` — 로컬 3-node NATS 클러스터
- `run-output.txt` — 실제 실행 결과 로그

## 재현 방법

```bash
go mod tidy
go build -o jetstreamConfigMigration.exe .
docker compose up -d
./jetstreamConfigMigration.exe 2>&1 | tee run-output.txt
docker compose down
```

## 기대 결과

- `WorkQueuePolicy -> InterestPolicy` direct mutation 실패
- consumer update 는 stream retention 제약을 해결하지 못함
- delete/recreate 는 상태를 초기화하므로 라이브 무손실 마이그레이션 경로로 unsafe 함

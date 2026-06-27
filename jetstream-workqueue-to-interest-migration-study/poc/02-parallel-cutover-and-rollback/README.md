# PoC 02 — Parallel Cutover and Rollback

이 PoC 는 in-place retention mutation 이 막힌 뒤, 더 안전한 대안으로서 병행 마이그레이션 경로를 검증합니다.

## 범위

다음 항목을 확인합니다.

1. 기존 WorkQueue 경로와 새 Interest 기반 경로가 공존할 수 있는가?
2. 새 consumer 를 늦게 붙여도 backlog bootstrap 이 가능한가?
3. shadow consumer 를 미리 만들어두면 더 안전한 staged cutover 가 가능한가?
4. old 경로를 유지하는 동안 rollback 이 가능한가?

## 파일 구성

- `main.go` — 테스트 드라이버
- `docker-compose.yaml`, `n1.conf`, `n2.conf`, `n3.conf` — 로컬 3-node NATS 클러스터
- `run-output.txt` — 실제 실행 결과 로그

## 재현 방법

```bash
go mod tidy
go build -o jetstreamConfigMigrationCutover.exe .
docker compose up -d
./jetstreamConfigMigrationCutover.exe 2>&1 | tee run-output.txt
docker compose down
```

## 기대 결과

- 병행 마이그레이션 자체는 가능함
- consumer 를 늦게 붙이는 전략은 bootstrap 가정 측면에서 unsafe 함
- shadow consumer 선생성은 더 안전한 컷오버 검증을 가능하게 함
- rollback 은 old 경로를 의도적으로 보존할 때만 현실적인 옵션이 됨

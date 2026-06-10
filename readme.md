NATS io의 개념을 poc 코드로 확인해 볼 수 있는 레포지토리입니다.

---

## PoC 검증 워크플로

새 개념을 검증할 때 아래 순서를 따른다.

### 1. 가설을 readme로 먼저 정리한다

코드보다 readme를 먼저 쓴다. 담아야 할 항목:

- **검증 목적** — "진짜 묻는 것"을 한 문장으로. 가설도 명시.
- **환경 표** — NATS 서버 버전, nats.go 버전, Storage, Consumer 종류, 주요 설정값.
- **공통 셋업** — 스트림 이름, subject, 사전 발행 수 등 모든 Phase가 공유하는 초기 상태.
- **Phase 별 표** — 각 Phase마다 시나리오 / 확인 포인트 / 기대값(가설) / 실제값(빈칸) / 결론(빈칸) 형식으로.
  - Phase 설계 팁: "기대값이 갈리는 조건"을 일부러 만든다. 예) OptStartSeq=1로 고정해 AckFloor와 충돌시키기.
  - 대조군(Phase D처럼 반대 경로) 하나를 반드시 포함한다.

### 2. 기존 PoC 구조를 참고해 파일을 만든다

같은 레포의 다른 PoC 디렉터리를 보고 아래 파일을 구성한다.

```
<poc-name>/
  main.go              # PoC 실행 코드
  go.mod / go.sum      # 의존성 (기존 프로젝트에서 복사 후 go mod tidy)
  docker-compose.yaml  # NATS 서버 버전 고정 (image: nats:<version>)
  nats.conf            # JetStream store_dir 설정
  readme.md            # 1단계에서 작성한 계획서
```

### 3. 코드를 구현한다

- Phase 순서대로 함수를 나눈다 (`phaseA`, `phaseB`, …).
- 각 단계마다 `ConsumerInfo`로 AckFloor·NumPending·NumAckPending을 로깅한다.
- 검증 포인트마다 기대값과 실제값을 비교해 `✓` / `✗`로 출력한다.
- 비자명한 설계 선택(예: `js.Publish` vs `nc.Publish`)은 한 줄 주석으로 이유를 남긴다.

### 4. 빌드 → 서버 기동 → 실행

```bash
go build -o <poc-name>.exe .
docker compose up -d
./<poc-name>.exe
```

### 5. 실행 결과를 readme에 기록한다

- 환경 표의 버전 칸을 실제 값으로 채운다.
- 각 Phase 표의 **실제값** / **결론** 칸을 채운다.
- 결과 요약 테이블도 채운다.

### 6. 커밋하고 전체 remote에 푸시한다

```bash
git add <poc-name>/
git commit -m "[chore]: <검증 내용 한 줄> PoC 추가 (NATS <version>)"
git push origin main
```

# 운영 중 신규 Account/User 추가 시 서버 재시작이 꼭 필요한가 PoC

인증/보안 강의 보강용 PoC. 사용 시나리오는 `resolver: MEMORY` + `resolver_preload` 기반 JWT 인증이다. 수강생이 가장 궁금해할 질문은 두 가지다.

1. **운영 중 기존 Account 아래에 User를 추가하면 서버 재시작이 필요한가?**
2. **운영 중 새로운 Account를 추가하면 서버 중지 없이 반영할 수 있는가?**

이 PoC는 NATS 2.12.2 + JWT auth + MEMORY resolver 기준으로 위 두 질문을 실측한다.

## 검증 목적

JWT 기반 NATS 인증 환경에서 다음이 가능한지 확인한다.

- **가설 H1** — 기존 Account(`SERVICEA`) 아래에 새 User를 추가하는 것은 서버 재시작이나 config reload 없이도 가능하다.
- **가설 H2** — 새로운 Account(`SERVICEB`)는 `resolver_preload`에 그 account JWT가 없으면 접속할 수 없다.
- **가설 H3** — 새로운 Account를 `resolver_preload`에 추가하고 **server reload**만 수행하면, **서버 재시작 없이** 접속 가능해진다.
- **가설 H4** — reload는 restart와 다르므로, 기존 연결과 컨테이너 시작 시각은 유지된다.

## 환경

| 항목 | 값 |
|---|---|
| NATS Server | 2.12.2 (`nats:2.12.2`) |
| nats.go | v1.52.0 |
| JWT 라이브러리 | `github.com/nats-io/jwt/v2` v2.8.0 |
| Resolver | `MEMORY` + `resolver_preload` |
| NATS config 반영 방식 | `docker compose kill -s HUP nats-server` reload |
| 기본 preload account | `SERVICEA` |
| 런타임 추가 account | `SERVICEB` |
| 기본 user | `servicea-user` |
| 런타임 추가 user(기존 account) | `servicea-user-2` |
| 런타임 추가 user(신규 account) | `serviceb-user` |

## 공통 셋업

- `./generated/auth/operator/operator.jwt` 에 operator JWT를 둔다.
- 초기 `nats.conf`는 `SERVICEA` account JWT만 `resolver_preload`에 넣는다.
- 모든 user는 creds 파일로 접속한다.
- subject 권한은 최소한으로 둔다.
  - `SERVICEA`: `_INBOX.>`, `servicea.>`, `svc.echo`
  - `SERVICEB`: `_INBOX.>`, `serviceb.>`, `svc.echo`

## 실행 방법

```bash
go build -o jwt-runtime-account-user-update.exe .
./jwt-runtime-account-user-update.exe --prepare
docker compose up -d
./jwt-runtime-account-user-update.exe 2>&1 | tee run-output.txt
docker compose down
```

`--prepare` 단계는 operator/account/user seed, jwt, creds, 초기 `nats.conf`를 생성한다.

## Phase 별 계획

### Phase A — baseline: preload된 SERVICEA user는 접속 가능해야 한다

| 항목 | 내용 |
|---|---|
| 시나리오 | 초기 preload에는 `SERVICEA`만 포함. `servicea-user` creds로 접속 시도 |
| 기대값 | 접속 성공 |
| 실제값 | `connect ok + publish ok` |
| 결론 | preload된 account/user는 정상 접속했다. |

### Phase B — 기존 Account 아래 신규 User 추가

| 항목 | 내용 |
|---|---|
| 시나리오 | 서버 실행 중 `SERVICEA` account seed로 `servicea-user-2` creds 생성 후 즉시 접속 시도 |
| 기대값 | 서버 재시작 / reload 없이 접속 성공 |
| 실제값 | `connect ok without reload` |
| 결론 | 기존 account를 서버가 이미 신뢰하고 있으면, 새 user는 재시작이나 reload 없이 즉시 접속 가능했다. |

### Phase C — 신규 Account 추가 전 접속 시도

| 항목 | 내용 |
|---|---|
| 시나리오 | 서버 실행 중 `SERVICEB` account + `serviceb-user` 생성. 하지만 `resolver_preload`는 아직 미갱신 |
| 기대값 | 접속 실패 |
| 실제값 | `nats: Authorization Violation` |
| 결론 | 서버가 모르는 신규 account는 user creds가 있어도 인증되지 않았다. |

### Phase D — 신규 Account를 `resolver_preload`에 추가 후 reload

| 항목 | 내용 |
|---|---|
| 시나리오 | `nats.conf`에 `SERVICEB` account JWT 추가 후 HUP reload 수행 |
| 기대값 | 컨테이너 재시작 없이 `serviceb-user` 접속 성공 |
| 실제값 | `connect ok after reload` |
| 결론 | 신규 account는 `resolver_preload` 반영 + reload만으로 활성화되었고, full restart는 필요하지 않았다. |

### Phase E — reload가 restart가 아니었는지 확인

| 항목 | 내용 |
|---|---|
| 시나리오 | reload 전/후 컨테이너 StartedAt 비교, 기존 `servicea-user` 연결에서 publish+flush 확인 |
| 기대값 | StartedAt 동일, 기존 연결 사용 가능 |
| 실제값 | `StartedAt(before=2026-06-25T14:58:40.552819851Z after=2026-06-25T14:58:40.552819851Z)`, 기존 연결 상태 `CONNECTED -> CONNECTED` |
| 결론 | reload는 restart가 아니었고, 기존 연결도 계속 사용 가능했다. |

## 결과 요약

| 검증 항목 | 실제 결과 | 가설 |
|---|---|---|
| 기존 Account 아래 신규 User 추가 | 재시작/reload 없이 즉시 접속 성공 | H1 성립 |
| 신규 Account preload 전 접속 | `Authorization Violation`으로 접속 실패 | H2 성립 |
| 신규 Account preload + reload 후 접속 | reload 뒤 즉시 접속 성공 | H3 성립 |
| reload 시 컨테이너 재시작 여부 / 기존 연결 유지 | StartedAt 동일, 기존 연결 `CONNECTED -> CONNECTED` | H4 성립 |

## 실제 실행 로그 포인트

- 신규 account 반영 전 서버 로그:
  - `Account fetch failed: account missing`
  - `authentication error`
- reload 시 서버 로그:
  - `Trapped "hangup" signal`
  - `Reloaded: authorization users`
  - `Reloaded: accounts`
  - `Reloaded server configuration`

즉, 이 PoC에서는 **신규 account를 모르는 상태에서 접속이 거부**되었고, **HUP reload 뒤에는 계정 정보가 재반영되어 즉시 접속이 허용**되었다.

## 컨테이너 내부 `nats.conf` / `resolver_preload` 확인

도커 컨테이너 안의 `/etc/nats/nats.conf`를 직접 복사해서 비교했다.

### 기존 Account 아래 새 User 추가 시

- baseline 컨테이너 파일 해시:
  - `11dd3ada44999d25c814d5658b5255a2098da2b5b4891491d509b36328a97646`
- `SERVICEA` 아래 새 User를 수동 생성하고 실제 접속까지 확인한 뒤 컨테이너 파일 해시:
  - `11dd3ada44999d25c814d5658b5255a2098da2b5b4891491d509b36328a97646`

즉, **새 User 추가만으로는 컨테이너 내부 `nats.conf`와 `resolver_preload`가 바뀌지 않았다.**

### 신규 Account 추가 후 reload 시

- 신규 Account 추가 전 컨테이너 파일 해시:
  - `11dd3ada44999d25c814d5658b5255a2098da2b5b4891491d509b36328a97646`
- 신규 Account를 `resolver_preload`에 반영하고 reload한 뒤 컨테이너 파일 해시:
  - `cd434181310928245914acf69dfb48e8cde306789a45009062bc4103b2479e0a`

reload 후 컨테이너 내부 `resolver_preload`는 다음처럼 **account 2개**를 담고 있었다.

```hocon
resolver_preload: {
  "<SERVICEA account pub>": "<SERVICEA account jwt>",
  "<SERVICEB account pub>": "<SERVICEB account jwt>"
}
```

즉, **신규 Account 추가는 실제로 컨테이너 내부 `nats.conf`의 `resolver_preload` 내용을 바꿨다.**

## 강의 반영 결론

1. **기존 Account 아래 새 User 추가**는, 서버가 이미 그 Account를 신뢰하는 상태라면 **재시작도 reload도 없이** 가능했다.
2. **새로운 Account 추가**는, 그 Account JWT를 서버가 아직 모르면 접속되지 않았다.
3. 하지만 `resolver: MEMORY` + `resolver_preload` 구성에서도, **config를 갱신하고 reload만 하면** 신규 account를 반영할 수 있었다. 즉 이 PoC 범위에서는 **restart까지는 필요하지 않았다.**
4. 따라서 강의에서는 `MEMORY라서 재시작하면 끝`이라고 단순화하기보다, **권한 원본을 어디에 두고 서버가 그걸 어떻게 reload하는지**가 핵심이라고 설명하는 편이 정확하다.
5. 단, 이 결과는 **단일 서버 + `resolver_preload` + HUP reload** PoC 결과다. 실제 강의에서 `nsc push`와 다중 노드 cluster 반영까지 일반화하려면, 각 노드의 반영 경로(예: 동일 conf 배포, 다른 resolver, 별도 account resolver 운용)를 분리해서 설명해야 한다.

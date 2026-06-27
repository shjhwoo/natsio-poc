# 운영 중 User 권한 추가/삭제가 restart 없이 반영되는가 PoC

JWT 기반 NATS 인증 환경에서, **특정 User의 subject 권한을 추가하거나 삭제할 때 서버 restart가 필요한지**를 단일 노드와 3노드 클러스터에서 각각 검증한다.

이번 PoC의 질문은 두 갈래다.

1. **기존 Account 아래 특정 User의 권한을 추가**하면, 서버 restart/reload 없이 새 권한이 적용되는가?
2. **특정 User의 기존 권한을 삭제**하면, 서버 restart/reload 없이 변경사항이 적용되는가?

이때 중요한 구분은 다음과 같다.

- **새로 다시 연결하는 클라이언트**는 변경된 user JWT를 사용해 접속한다.
- **이미 연결돼 있던 기존 세션**은 연결 시점의 권한 상태를 계속 유지할 수도 있다.

따라서 이 PoC는 **재연결 없이 기존 연결에 권한이 즉시 바뀌는지**와, **새 연결에는 restart 없이 새 권한이 적용되는지**를 구분해서 본다.

## 검증 범위

- Topology A: **single node**
- Topology B: **3-node cluster**
- Auth model: `operator` + `resolver: MEMORY` + `resolver_preload`
- Account: `SERVICEA`
- User: `servicea-user`
- 테스트 subject
  - baseline 허용: `servicea.base`
  - 나중에 추가되는 권한: `servicea.extra`

## 가설

- **H1** — 같은 Account 아래에서 **user JWT만 바꿔 새 creds로 재연결**하면, 서버 restart/reload 없이도 **추가된 권한**이 적용된다.
- **H2** — 같은 Account 아래에서 **user JWT에서 권한을 제거하고 새 creds로 재연결**하면, 서버 restart/reload 없이도 **삭제된 권한**이 적용된다.
- **H3** — **이미 연결된 기존 세션**은 연결 시점의 권한 상태를 계속 유지할 수 있다. 즉, creds 파일을 새로 만들었다고 해서 기존 연결이 자동으로 재권한평가되지는 않을 수 있다.
- **H4** — 위 동작은 단일 노드뿐 아니라, **동일한 account preload를 가진 3-node cluster의 각 노드 직접 접속**에서도 동일하게 관찰된다.

## 실행 방식

### Single node

```bash
go build -o jwt-user-permission-mutation.exe .
./jwt-user-permission-mutation.exe --prepare
docker compose -f docker-compose.single.yaml up -d
./jwt-user-permission-mutation.exe --topology single 2>&1 | tee run-single.txt
docker compose -f docker-compose.single.yaml down
```

### 3-node cluster

```bash
./jwt-user-permission-mutation.exe --prepare
docker compose -f docker-compose.cluster.yaml up -d
./jwt-user-permission-mutation.exe --topology cluster 2>&1 | tee run-cluster.txt
docker compose -f docker-compose.cluster.yaml down
```

## Phase 계획

### 공통 Phase 1 — baseline
- v1 creds: `servicea.base`만 허용
- 기대값:
  - `servicea.base` → 허용
  - `servicea.extra` → 거부

### 공통 Phase 2 — 권한 추가
- v2 creds: `servicea.base` + `servicea.extra` 허용
- 기대값:
  - **기존 v1 연결**은 계속 `servicea.extra` 거부 가능
  - **새 v2 연결**은 restart/reload 없이 `servicea.extra` 허용

### 공통 Phase 3 — 권한 삭제
- v3 creds: `servicea.extra`만 허용 (`servicea.base` 제거)
- 기대값:
  - **기존 v2 연결**은 계속 `servicea.base` 허용 가능
  - **새 v3 연결**은 restart/reload 없이 `servicea.base` 거부, `servicea.extra` 허용

## 결과 기록 표 — Single

| 검증 항목 | 실제 결과 | 가설 |
|---|---|---|
| baseline v1: base 허용 / extra 거부 | `pub(base=allow extra=deny) sub(base=allow extra=deny)` | H1/H2 전제 확인 |
| 권한 추가 후 기존 v1 연결의 extra | 여전히 거부 (`pub/sub extra=deny`) | H3 성립 |
| 권한 추가 후 새 v2 연결의 extra | 즉시 허용 (`pub/sub extra=allow`) | H1 성립 |
| 권한 삭제 후 기존 v2 연결의 base | 여전히 허용 (`pub/sub base=allow`) | H3 성립 |
| 권한 삭제 후 새 v3 연결의 base/extra | `base=deny`, `extra=allow` | H2 성립 |

## 결과 기록 표 — Cluster

| 검증 항목 | 실제 결과 | 가설 |
|---|---|---|
| node1 baseline v1 | `pub(base=allow extra=deny) sub(base=allow extra=deny)` | H4 성립 |
| node2 baseline v1 | `pub(base=allow extra=deny) sub(base=allow extra=deny)` | H4 성립 |
| node3 baseline v1 | `pub(base=allow extra=deny) sub(base=allow extra=deny)` | H4 성립 |
| 권한 추가 후 node1/node2/node3 새 v2 연결 | 세 노드 모두 `pub/sub extra=allow` | H1/H4 성립 |
| 권한 삭제 후 node1/node2/node3 새 v3 연결 | 세 노드 모두 `pub/sub base=deny`, `extra=allow` | H2/H4 성립 |
| 권한 삭제 후 기존 v2 연결 유지 여부(대표 연결) | 기존 연결은 계속 `pub/sub base=allow extra=allow` | H3/H4 성립 |

## 강의용 예상 포인트

이 PoC가 그대로 확인되면 강의에서 이렇게 말할 수 있다.

- **기존 Account 아래에서 User 권한을 바꾸는 것 자체**는 서버 restart가 꼭 필요한 작업이 아니다.
- 다만 **기존에 이미 연결된 세션의 권한이 자동으로 바뀌는가**는 별도 문제다.
- 실무적으로는 **새 JWT로 재연결시키거나, 필요하면 기존 세션을 정리해서 재인증시키는 운영 전략**까지 같이 설명해야 한다.

## 실제 결론

실제 실행 결과는 단일 노드와 3노드 cluster 모두에서 동일한 방향으로 나왔다.

1. **같은 Account 아래에서 User 권한을 추가/삭제하는 것 자체는 서버 restart나 reload가 없어도 새 연결에는 반영되었다.**
2. 하지만 **이미 연결되어 있던 기존 세션은 자동으로 새 권한으로 바뀌지 않았다.**
   - v1 연결은 v2 권한 추가 뒤에도 `servicea.extra`를 계속 거부했다.
   - v2 연결은 v3 권한 삭제 뒤에도 `servicea.base`를 계속 허용했다.
3. 따라서 운영 관점에서는 이렇게 설명하는 게 정확하다.
   - **권한 변경 자체**: restart 불필요
   - **기존 세션까지 새 권한 강제 적용**: 재연결 또는 세션 정리가 필요할 수 있음

## 로그에서 확인한 포인트

- baseline/v1에서 `servicea.extra`에 대해 각 노드가 실제로 다음과 같은 위반 로그를 남겼다.
  - `Publish Violation - Subject "servicea.extra"`
  - `Subscription Violation - Subject "servicea.extra"`
- v3(권한 삭제 후 새 creds)에서는 각 노드가 `servicea.base`에 대해 다음 로그를 남겼다.
  - `Publish Violation - Subject "servicea.base"`
  - `Subscription Violation - Subject "servicea.base"`

즉, 이번 PoC는 단순히 클라이언트 쪽 성공/실패만 본 것이 아니라, **서버 로그 레벨에서도 권한 추가/삭제가 새 연결에 즉시 반영되는 것**을 확인했다.

## 컨테이너 내부 `nats.conf` / `resolver_preload` 확인

이번 PoC에서는 user JWT/creds만 바꾸고, account preload 자체는 건드리지 않았다. 그래서 도커 컨테이너 안의 `/etc/nats/nats.conf`를 직접 복사해 **실제로 해시가 바뀌는지** 확인했다.

### Single node

- before:
  - `65959f3ae9feac9ef34c82ac837d147815ced44855528026aa9e90e36a28be15`
- after:
  - `65959f3ae9feac9ef34c82ac837d147815ced44855528026aa9e90e36a28be15`

즉, **1노드에서 User pub/sub 권한을 추가/삭제해도 컨테이너 내부 `nats.conf` / `resolver_preload`는 바뀌지 않았다.**

### 3-node cluster

- n1 before/after:
  - `9714909ec222616e553e3f4b1f87d66286de45e9281f4deb1ee9499022c9b9e2`
  - `9714909ec222616e553e3f4b1f87d66286de45e9281f4deb1ee9499022c9b9e2`
- n2 before/after:
  - `0b72d88a2240ab58a5e8df63b9853b52e1ced5c3c786a4b996c0b94f482c3005`
  - `0b72d88a2240ab58a5e8df63b9853b52e1ced5c3c786a4b996c0b94f482c3005`
- n3 before/after:
  - `47ed16eec57089b8757dee13076f0164875d203ef9ee2e27407d574ad2e61b3d`
  - `47ed16eec57089b8757dee13076f0164875d203ef9ee2e27407d574ad2e61b3d`

즉, **3노드 cluster에서도 User 권한 변경만으로는 각 노드의 컨테이너 내부 `nats.conf` / `resolver_preload`가 바뀌지 않았다.**

해석은 명확하다.

- `resolver_preload`는 **account JWT preload**를 담는 자리다.
- **새 User 추가**나 **기존 User pub/sub 권한 변경**은 user JWT/creds 레벨의 변화다.
- 따라서 이번 구조에서는 **User 변화가 `resolver_preload`를 직접 바꾸지 않았다.**

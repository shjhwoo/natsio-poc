# Ephemeral 재사용 시 AckFloor가 OptStartSeq 중복을 막는가 — PoC 검증 계획서

## 검증 목적

> "InactiveThreshold 안에 재접속하면 기존 Ephemeral 컨슈머가 재사용된다.
> 그런데 OptStartSeq는 컨슈머 생성 시점에 고정된 값이다.
> 그럼 재접속할 때마다 그 고정된 시작점부터 다시 받아서 중복이 나지 않을까?
> 가설: 아니다. 컨슈머가 살아있는 동안엔 AckFloor가 있어서,
> OptStartSeq가 고정이어도 이미 ack한 메시지는 다시 오지 않는다."

**진짜 묻는 것:** 살아있는 컨슈머의 재전송 시작점을 결정할 때, OptStartSeq(고정 생성값)와
AckFloor(런타임에 올라가는 ack 바닥선) 중 **누가 우선하는가**.
가설은 "AckFloor가 우선한다 → 중복 없음".

> 주의: 이건 "컨슈머 재생성" 시나리오가 아니다. **같은 컨슈머 재사용**(InactiveThreshold 내 재접속)이 핵심.
> 재생성 경로(컨슈머가 만료된 뒤 새로 만드는 경우)는 별도이며, 그땐 AckFloor가 없으니 OptStartSeq가 유일 기준.

---

## 환경

| 항목              | 값                                                                  |
| ----------------- | ------------------------------------------------------------------- |
| NATS Server       | 2.12.2                                                              |
| nats.go           | v1.52.0                                                             |
| Storage           | FileStorage 권장(+ MemoryStorage 1회 교차)                          |
| Consumer          | Ephemeral (이름 없음 또는 Named 둘 다 권장 — 13-A강 결론과 교차)    |
| DeliverPolicy     | DeliverByStartSequencePolicy, OptStartSeq 고정                      |
| AckPolicy         | AckExplicitPolicy (AckFloor를 만들려면 필수)                        |
| InactiveThreshold | 넉넉히(예: 5m) — "임계값 내 재접속=재사용"을 안정적으로 만들기 위함 |

> 핵심 설계: **OptStartSeq를 일부러 낮게(예: 1) 고정**한다.
> 그래야 "고정 시작점(1)을 따르면 중복, AckFloor를 따르면 중복 없음"이 갈려서 누가 이기는지 보인다.

---

## 공통 셋업

```
Stream: REUSE_TEST
Subject: chat.user.a
사전 발행: seq 1~10 (payload에 stream seq 박아 로깅)
Ephemeral 컨슈머 생성: DeliverByStartSequence, OptStartSeq=1, AckExplicit, InactiveThreshold=5m
```

> LastSeqNo는 msg.Metadata().Sequence.Stream 사용(컨슈머 seq 아님).

---

## Phase A — 기준 동작: 같은 컨슈머가 재사용되는지부터 확정

|                  |                                                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | 컨슈머 생성(OptStartSeq=1) → seq 1~5 수신하고 **모두 ack** → 연결만 끊음(Consume만 멈춤, 컨슈머는 삭제 안 함) → InactiveThreshold(5m) 안에 재접속 |
| **확인 포인트**  | ① 재접속 시 같은 컨슈머가 살아있나 (ConsumerInfo의 Created 시각·이름·ID 동일?) ② AckFloor(=AckFloor seq)가 5로 유지돼 있나                        |
| **기대값(가설)** | 컨슈머 동일 인스턴스로 재사용. AckFloor=5 보존                                                                                                    |
| **실제값**       | name·Created 완전 동일, AckFloor.Stream=5, NumPending=5(seq 6~10), NumAckPending=0                                                               |
| **결론**         | ✓ 확인. InactiveThreshold(5m) 내 재접속 시 컨슈머가 재사용되고 AckFloor가 그대로 보존된다.                                                        |

> 먼저 "정말 재사용된다"를 ConsumerInfo로 못박아야, 다음 Phase의 해석이 명확해진다.

## Phase B — ★핵심: OptStartSeq 고정인데 AckFloor가 중복을 막는가

|                  |                                                                                                               |
| ---------------- | ------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | (Phase A에 이어) 끊긴 동안 seq 6~10이 이미 스트림에 있음 → 재접속하여 다시 Consume 시작                       |
| **확인 포인트**  | 재접속 후 들어오는 첫 메시지가 **6번인가(=AckFloor+1)**, 아니면 **1번인가(=OptStartSeq)**? 1~5가 다시 오는가? |
| **기대값(가설)** | 6~10만 수신. 1~5 재수신 0건. → OptStartSeq가 1로 고정이어도 AckFloor(5)가 우선 → 중복 없음                    |
| **실제값**       | 첫 수신 seq=6, 수신 목록=[6 7 8 9 10], 1~5 재수신 0건                                                        |
| **결론**         | ✓ 확인. AckFloor(5)가 OptStartSeq(1)를 완전히 무시하고 우선한다. 중복 없음.                                   |

> 이 Phase 하나가 강의의 핵심 증거. "OptStartSeq 고정 = 중복" 오해를 AckFloor로 정면 반박.

## Phase C — 부분 ack 경계: ack한 것까지만 건너뛰는가

|                  |                                                                                                          |
| ---------------- | -------------------------------------------------------------------------------------------------------- |
| **시나리오**     | OptStartSeq=1로 생성 → 1~5 수신 후 **3번까지만 ack**, 4·5는 받았지만 ack 안 함 → 끊김 → 임계값 내 재접속 |
| **확인 포인트**  | 재접속 후 4번부터 오는가? (AckFloor=3 → 4부터) 1~3은 안 오고, 4·5는 미ack라 재전송되는가?                |
| **기대값(가설)** | 4번부터 재개. 1~3 재수신 0. 4·5는 미ack였으니 다시 옴(중복이 아니라 정상 재전송)                         |
| **실제값**       | AckFloor.Stream=3, NumAckPending=2. 재접속 후 첫 수신 seq=4, 수신 목록=[4 5 6 7 8 9 10], 1~3 재수신 0건 |
| **결론**         | ✓ 확인. AckFloor=연속으로 ack된 최고 지점(3). 4·5는 미ack 재전송(at-least-once 정상 동작), 중복 아님.   |

> "중복처럼 보이는 것"과 "정상 미ack 재전송"을 구분. AckFloor는 '연속으로 ack된 최고 지점'임을 드러냄.

## Phase D — 대조군: 컨슈머가 만료된 뒤 재생성하면? (AckFloor 사라짐)

|                  |                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **시나리오**     | InactiveThreshold를 짧게(예: 5s)로 바꿔, 1~5 ack 후 끊고 임계값 **초과** 대기 → 컨슈머 자동 삭제됨 → 새로 OptStartSeq=1로 재생성 |
| **확인 포인트**  | 이번엔 1번부터 다시 오는가? (AckFloor가 사라졌으니 OptStartSeq=1이 유일 기준)                                                    |
| **기대값(가설)** | 1~10 전부 다시 수신 = 중복 발생. → "재사용이면 AckFloor가 막지만, 재생성이면 못 막는다"는 대조                                   |
| **실제값**       | 신규 컨슈머 AckFloor.Stream=0, NumPending=10. 첫 수신 seq=1, 수신 목록=[1 2 3 4 5 6 7 8 9 10]                                   |
| **결론**         | ✓ 확인. AckFloor 소멸 → OptStartSeq(1)가 유일 기준. 1~5 중복 발생. 재생성 경로에선 클라이언트가 LastSeqNo+1을 직접 줘야 한다.   |

> Phase B(재사용=중복없음) vs Phase D(재생성=중복) 대조가 결론을 못박는다.
> 그래서 재생성 경로에선 LastSeqNo+1을 클라이언트가 직접 줘야 함(앞선 DeliverByStartSeq PoC와 연결).

---

## 결과 요약 (실험 후 채움)

| 케이스              | 재접속 후 첫 수신 seq | 1~5 중복?      | 해석                      |
| ------------------- | --------------------- | -------------- | ------------------------- |
| B: 재사용·전부 ack  | **6** ✓               | **없음** ✓     | AckFloor(5)>OptStartSeq(1) |
| C: 재사용·3까지 ack | **4** ✓               | **1~3 없음** ✓ | AckFloor=연속 ack 최고점(3), 4·5는 미ack 정상 재전송 |
| D: 재생성(만료 후)  | **1** ✓               | **중복** ✓     | AckFloor 소멸→OptStartSeq(1) 유일 기준 |

---

## 강의 반영 결론 후보 (PoC 후 확정)

- **재사용 경로**(InactiveThreshold 내 재접속): OptStartSeq가 생성 시 고정값이어도,
  살아있는 컨슈머의 AckFloor가 우선하므로 이미 ack한 메시지는 다시 오지 않는다 → 중복 없음(B).
  → "OptStartSeq 고정이라 중복 나지 않냐"는 오해를 정면 반박.
- 미ack 메시지는 재전송되지만 이건 중복이 아니라 at-least-once의 정상 동작(C).
- **재생성 경로**(컨슈머 만료 후 새로 생성): AckFloor가 사라지므로 OptStartSeq가 유일 기준 →
  이때는 클라이언트가 LastSeqNo+1을 줘야 중복을 피한다(D).
- 한 줄 정리: 짧은 끊김=컨슈머 재사용=서버 AckFloor가 시작점을 책임(클라이언트 OptStartSeq 무관),
  긴 끊김=재생성=클라이언트가 LastSeqNo+1로 시작점 제공.

## 실행 메모

- ConsumerInfo로 매 단계 AckFloor(AckFloorSequence)·NumAckPending·재사용 여부를 로깅.
- OptStartSeq는 반드시 1 이상(0이면 오류). 일부러 1로 둬서 AckFloor와 충돌시키는 게 이 PoC의 묘미.
- 결과 확정 후: 12강의 "재연결 중복 오해" 한 단락 + 13강 실습 시연으로 이식.

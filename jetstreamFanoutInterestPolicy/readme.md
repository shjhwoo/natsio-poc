# JetStream Fanout + InterestPolicy

## 목적

기존 `queueGroupWithFanout`은 Core NATS pub/sub 기반이라 **서비스 다운타임 동안 발행된 메시지가 유실**됩니다.
이 PoC는 동일한 fanout + queue group 패턴을 **JetStream + `InterestPolicy`** 로 구현하여:

1. fanout (모든 서비스가 메시지 수신)
2. 서비스 내 단일 인스턴스 처리 (queue group 효과)
3. **모든 서비스가 ack해야 stream에서 삭제** (메시지 유실 방지)
4. **NATS 서버 재시작 후에도 메시지 보존**

이 네 가지를 동시에 만족함을 검증합니다.

## 핵심 메커니즘

### Core NATS → JetStream 매핑

| Core NATS (`queueGroupWithFanout`) | JetStream (이 PoC) |
|---|---|
| 1 Subject (휘발) | **1 Stream** (`Storage=File`로 영속) |
| Service = Queue group 이름 (`QueueSubscribe`) | Service = **Durable Consumer 1개** |
| Instance = 같은 queue group 멤버 | Instance = 같은 durable name으로 `CreateOrUpdateConsumer` 호출 (자동 work-share) |
| Fanout = 다른 queue group | Fanout = 다른 durable consumer (각자 ack state 보유) |
| 메시지 휘발 | `InterestPolicy` + `FileStorage` |

### Retention Policy 비교

| Retention | 메시지 삭제 시점 | 이 PoC 적합도 |
|---|---|---|
| `LimitsPolicy` (default) | age/size limit 도달 시 | × (ack 무관하게 보존) |
| `WorkQueuePolicy` | 단 하나의 consumer가 ack하면 즉시 | × (fanout 불가) |
| **`InterestPolicy`** | **interest 있는 모든 consumer가 ack해야** | **○** |

`InterestPolicy`는 stream이 등록된 모든 durable consumer의 ack 상태를 추적하여,
**전부 ack한 메시지만** 삭제합니다. 이게 "모든 서비스 처리 완료 후 자동 삭제" 요구를 정확히 충족합니다.

## 실행 방법

```bash
# 1. NATS 서버 실행 (JetStream 활성화 + 영속 볼륨)
docker compose up -d

# 2. PoC 실행
go run main.go

# 3. Phase 4 안내가 뜨면 별도 터미널에서 실행 후 원래 터미널에서 Enter
docker compose restart nats-hub-server-1
```

## 검증 시나리오

전체 흐름은 4개 phase로 구성됩니다. 각 phase 끝에 `Stream.Msgs`(stream에 남은 메시지 수)와
각 consumer의 `NumPending`(아직 클라이언트에 전달 안 된 수)을 출력합니다.

| Phase | 동작 | 검증 포인트 |
|---|---|---|
| **1. Baseline** | A/B/C 모두 가동 → 5 메시지 발행 | `Stream.Msgs=0` (전부 ack 완료) |
| **2. 부분 ack 후 보존** | C 정지 → 5 메시지 발행 → A/B만 ack | `Stream.Msgs=5` 보존, `C.NumPending=5` |
| **3. 복구 후 자동 삭제** | C 재시작 → 밀린 5개 처리 | `Stream.Msgs=0` (모두 ack 완료 → InterestPolicy로 삭제) |
| **4. 서버 재시작 영속성** | 모든 서비스 정지 → 5 메시지 발행 → docker restart | 재시작 전후 `Stream.Msgs=5` 유지, 재가동 후 정상 처리 |

## 실행 결과 예시

### Phase 1: Baseline

```
========== Phase 1: Baseline ==========
>> 5개 메시지 발행 (prefix=P1)
[처리] serviceA-2 | durable=QueueGroupA | data=P1-Event-002
[처리] serviceC-3 | durable=QueueGroupC | data=P1-Event-003
[처리] serviceB-2 | durable=QueueGroupB | data=P1-Event-002
... (각 메시지가 3개 서비스 × 1 인스턴스 = 총 15회 처리)
┌─ Stream 상태 ──────────────────────────────
│  Stream.Msgs = 0
│  [serviceA/QueueGroupA] NumPending=0, NumAckPending=0
│  [serviceB/QueueGroupB] NumPending=0, NumAckPending=0
│  [serviceC/QueueGroupC] NumPending=0, NumAckPending=0
└────────────────────────────────────────────
```

같은 메시지(`P1-Event-001`)가 **3개 서비스에서 각 1번씩** 처리됨 = fanout O.
같은 서비스 내에서는 1개 인스턴스만 처리 = queue group 효과 O.

### Phase 2: serviceC 정지 후 메시지 발행

```
========== Phase 2: 부분 ack 후 보존 ==========
>> serviceC 정지 완료
>> 5개 메시지 발행 (prefix=P2)
[처리] serviceA-* / serviceB-* | data=P2-Event-...   (A/B만 처리, C는 정지 상태)
┌─ Stream 상태 ──────────────────────────────
│  Stream.Msgs = 5                               ← 핵심! C가 ack 안 했으므로 보존
│  [serviceA/QueueGroupA] NumPending=0, NumAckPending=0
│  [serviceB/QueueGroupB] NumPending=0, NumAckPending=0
│  [serviceC/QueueGroupC] NumPending=5, NumAckPending=0   ← C에게 5개 대기 중
└────────────────────────────────────────────
```

`consumeCtx.Stop()`은 클라이언트 측 풀링만 멈추고 **durable consumer는 서버에 남아있음**.
따라서 InterestPolicy 입장에서는 "C가 아직 interest 있다"로 인식 → 메시지 보존.

### Phase 3: serviceC 복구 후 자동 삭제

```
========== Phase 3: 복구 후 자동 삭제 ==========
>> serviceC 시작 (durable=QueueGroupC, instances=3)
[처리] serviceC-1 | data=P2-Event-001
[처리] serviceC-2 | data=P2-Event-002
... (밀린 5개 처리)
┌─ Stream 상태 ──────────────────────────────
│  Stream.Msgs = 0       ← C까지 ack 완료 → InterestPolicy가 자동 삭제
│  [serviceC/QueueGroupC] NumPending=0, NumAckPending=0
└────────────────────────────────────────────
```

### Phase 4: 서버 재시작 영속성

```
========== Phase 4: 서버 재시작 영속성 ==========
>> 모든 서비스 정지 완료
>> 5개 메시지 발행 (prefix=P4)
>> 재시작 직전 stream 상태:
│  Stream.Msgs = 5
│  [모두] NumPending=5

>>> docker compose restart nats-hub-server-1 실행 후 Enter

>> NATS 연결 끊김: EOF
>> NATS 재연결 성공: nats://localhost:4222
>> 재시작 직후 stream 상태 (메시지가 보존되어야 함):
│  Stream.Msgs = 5      ← 서버 재시작에도 메시지 보존! (FileStorage)
│  [모두] NumPending=5

>> 모든 서비스 재시작 → 메시지 처리...
[처리] serviceA-1 | data=P4-Event-001
... (전부 처리)
│  Stream.Msgs = 0      ← 최종적으로 모두 ack → 삭제
```

## 결론

| 요구사항 | 충족 여부 | 메커니즘 |
|---|---|---|
| 각 서비스가 모두 같은 메시지 수신 (fanout) | ✓ | 서비스별 별도 durable consumer |
| 서비스 내 단일 인스턴스만 처리 | ✓ | 같은 durable name 공유 (`CreateOrUpdateConsumer`) |
| 모든 서비스 ack 후 stream에서 삭제 | ✓ | `Retention: InterestPolicy` |
| 서비스 다운타임 시 메시지 유실 방지 | ✓ | durable consumer가 서버에 남아 interest 유지 |
| NATS 서버 재시작 후 메시지 보존 | ✓ | `Storage: FileStorage` + docker volume |

Core NATS pub/sub의 fanout 패턴을 **메시지 유실 보장과 함께** JetStream으로 안전하게 옮길 수 있음을 확인했습니다.

## 주요 코드 포인트

```go
// 1. Stream: InterestPolicy + FileStorage
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
    Name:      "eventSink",
    Subjects:  []string{"starfruit.internal.event"},
    Retention: jetstream.InterestPolicy,  // 핵심
    Storage:   jetstream.FileStorage,     // 영속성
})

// 2. 서비스별 Durable Consumer (인스턴스들이 같은 durable로 공유 → queue group 효과)
stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
    Durable:       "QueueGroupA",            // 서비스별 고유, 인스턴스 간 공유
    AckPolicy:     jetstream.AckExplicitPolicy,
    FilterSubject: "starfruit.internal.event",
    AckWait:       30 * time.Second,
})

// 3. 재연결 옵션 (서버 재시작 대응)
nats.Connect(natsURL,
    nats.MaxReconnects(-1),
    nats.ReconnectWait(2*time.Second),
)
```

## 관련 PoC

- [queueGroupWithFanout](../queueGroupWithFanout/readme.md) — 동일 패턴의 Core NATS 버전 (메시지 유실 가능)

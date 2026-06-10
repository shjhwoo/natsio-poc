package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL    = "nats://localhost:4222"
	subject    = "chat.user.a"
	streamName = "REUSE_TEST"
)

func main() {
	log.Println("=== AckFloor vs OptStartSeq PoC ===")
	log.Println("검증: InactiveThreshold 내 재접속 시 AckFloor가 OptStartSeq 중복을 막는가")

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()

	log.Printf(">> 접속된 NATS 서버 버전: %s", nc.ConnectedServerVersion())

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	// ======================== 공통 셋업 ========================
	log.Println("\n========== 공통 셋업 ==========")
	_ = js.DeleteStream(ctx, streamName)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream '%s' 생성 (FileStorage, LimitsPolicy)", streamName)

	log.Println(">> seq 1~10 사전 발행")
	// nc.Publish 대신 js.Publish를 쓰는 이유: PubAck에서 서버가 부여한 stream_seq를 바로 확인하기 위함.
	// 기능 차이는 없고 setup 로깅 용도.
	for i := 1; i <= 10; i++ {
		pubAck, err := js.Publish(ctx, subject, []byte(fmt.Sprintf("msg-%03d", i)))
		if err != nil {
			log.Fatalf("발행 실패: %v", err)
		}
		log.Printf("   → stream_seq=%d, payload=msg-%03d", pubAck.Sequence, i)
	}

	// ======================== Phase A + B ========================
	phaseAB(ctx, stream)

	// ======================== Phase C ========================
	phaseC(ctx, stream)

	// ======================== Phase D ========================
	phaseD(ctx, stream)

	log.Println("\n=== PoC 종료 ===")
	log.Println("\n결과 요약 포인트:")
	log.Println("  B(재사용·전부 ack):  첫 수신이 6이면 → AckFloor 우선, 중복 없음")
	log.Println("  C(재사용·3까지 ack): 첫 수신이 4이면 → AckFloor=연속 ack 최고점, 1~3 중복 없음")
	log.Println("  D(재생성·만료 후):   첫 수신이 1이면 → AckFloor 소멸, OptStartSeq=1이 유일 기준, 중복")
}

// phaseAB runs Phase A (consumer reuse verification) and Phase B (AckFloor beats OptStartSeq).
func phaseAB(ctx context.Context, stream jetstream.Stream) {
	log.Println("\n========== Phase A: 컨슈머 재사용 확인 ==========")
	log.Println("시나리오: OptStartSeq=1 컨슈머 생성 → seq 1~5 수신·ack → '끊음' → InactiveThreshold 내 재접속")

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("[A] 컨슈머 생성 실패: %v", err)
	}

	info, err := consumer.Info(ctx)
	if err != nil {
		log.Fatalf("[A] ConsumerInfo 실패: %v", err)
	}
	consumerName := info.Name
	consumerCreated := info.Created
	log.Printf("[A] 컨슈머 생성됨: name=%s", consumerName)
	log.Printf("[A]   Created=%v", consumerCreated.Format(time.RFC3339Nano))
	log.Printf("[A]   초기 AckFloor: stream=%d, consumer=%d", info.AckFloor.Stream, info.AckFloor.Consumer)

	// seq 1~5 수신 및 모두 ack
	log.Println("[A] seq 1~5 수신 및 ack 중...")
	msgs, err := consumer.Fetch(5, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		log.Fatalf("[A] Fetch 실패: %v", err)
	}
	for msg := range msgs.Messages() {
		meta, _ := msg.Metadata()
		log.Printf("[A]   수신·ack: stream_seq=%d, payload=%s", meta.Sequence.Stream, string(msg.Data()))
		if err := msg.Ack(); err != nil {
			log.Printf("[A]   Ack 실패: %v", err)
		}
	}
	if err := msgs.Error(); err != nil {
		log.Printf("[A] msgs error: %v", err)
	}

	time.Sleep(300 * time.Millisecond) // ack 서버 반영 대기

	info, _ = consumer.Info(ctx)
	log.Printf("[A] ack 후 AckFloor: stream=%d (기대: 5)", info.AckFloor.Stream)
	log.Printf("[A] NumPending=%d (기대: 5 — seq 6~10), NumAckPending=%d (기대: 0)", info.NumPending, info.NumAckPending)

	// "끊음" 시뮬레이션 — InactiveThreshold(5m) 훨씬 이내
	log.Println("[A] '연결 끊음' 시뮬레이션 — 3초 대기 (InactiveThreshold=5m 이내)")
	time.Sleep(3 * time.Second)

	// 재접속: 같은 이름으로 컨슈머 다시 조회
	log.Printf("[A] 재접속 — stream.Consumer('%s') 호출", consumerName)
	reconnected, err := stream.Consumer(ctx, consumerName)
	if err != nil {
		log.Fatalf("[A] 재접속 실패(컨슈머 조회 실패): %v", err)
	}

	info2, err := reconnected.Info(ctx)
	if err != nil {
		log.Fatalf("[A] 재접속 후 ConsumerInfo 실패: %v", err)
	}

	log.Println("[A] ★ Phase A 결과 ★")
	sameName := consumerName == info2.Name
	sameCreated := consumerCreated.Equal(info2.Created)
	log.Printf("[A]   name 동일: %v (%s)", sameName, info2.Name)
	log.Printf("[A]   Created 동일: %v (%v)", sameCreated, info2.Created.Format(time.RFC3339Nano))
	log.Printf("[A]   AckFloor.Stream: %d (기대: 5)", info2.AckFloor.Stream)
	log.Printf("[A]   NumPending: %d (기대: 5)", info2.NumPending)

	if sameName && sameCreated && info2.AckFloor.Stream == 5 {
		log.Println("[A]   → ✓ 같은 컨슈머 재사용 확인, AckFloor=5 보존")
	} else {
		log.Println("[A]   → ✗ 예상과 다름 — 다음 Phase 해석에 주의 필요")
	}

	// Phase B
	log.Println("\n========== Phase B: AckFloor가 OptStartSeq보다 우선하는가 ==========")
	log.Println("[B] 시나리오: 재접속 후 Consume 재개 → 첫 수신이 seq 6(AckFloor+1)인가 seq 1(OptStartSeq)인가?")
	log.Println("[B] 기대: 6부터 수신, 1~5 재수신 없음 → AckFloor 우선")

	var received []uint64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		batch, err := reconnected.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			break
		}
		got := false
		for msg := range batch.Messages() {
			meta, _ := msg.Metadata()
			received = append(received, meta.Sequence.Stream)
			log.Printf("[B]   수신: stream_seq=%d, payload=%s", meta.Sequence.Stream, string(msg.Data()))
			msg.Ack()
			got = true
		}
		if !got {
			break
		}
	}

	log.Println("[B] ★ Phase B 결과 ★")
	if len(received) == 0 {
		log.Println("[B]   수신된 메세지 없음")
	} else {
		firstSeq := received[0]
		switch {
		case firstSeq == 6:
			log.Printf("[B]   첫 수신 seq: %d → ✓ AckFloor(5)+1=6. OptStartSeq(1) 무시. 중복 없음!", firstSeq)
		case firstSeq == 1:
			log.Printf("[B]   첫 수신 seq: %d → ✗ OptStartSeq(1) 우선. 중복 발생!", firstSeq)
		default:
			log.Printf("[B]   첫 수신 seq: %d → 예상 외", firstSeq)
		}
		hasDup := false
		for _, s := range received {
			if s <= 5 {
				hasDup = true
			}
		}
		if hasDup {
			log.Printf("[B]   ✗ 1~5 재수신됨 (중복) — received=%v", received)
		} else {
			log.Printf("[B]   ✓ 1~5 재수신 없음 — received=%v", received)
		}
	}
}

// phaseC verifies partial-ack boundary: only acked-through-3, so AckFloor=3, expect resume at seq 4.
func phaseC(ctx context.Context, stream jetstream.Stream) {
	log.Println("\n========== Phase C: 부분 ack 경계 ==========")
	log.Println("시나리오: OptStartSeq=1 신규 컨슈머 → seq 1~5 수신 → 3번까지만 ack → 4·5는 NAK → 재접속")

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("[C] 컨슈머 생성 실패: %v", err)
	}

	info, _ := consumer.Info(ctx)
	consumerName := info.Name
	log.Printf("[C] 신규 컨슈머: name=%s", consumerName)

	// seq 1~5를 한 번에 fetch — NAK 후 즉시 재전송 레이스를 피하기 위해 배치로 수신
	msgs, err := consumer.Fetch(5, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		log.Fatalf("[C] Fetch 실패: %v", err)
	}
	for msg := range msgs.Messages() {
		meta, _ := msg.Metadata()
		seq := meta.Sequence.Stream
		if seq <= 3 {
			msg.Ack()
			log.Printf("[C]   수신·ack: stream_seq=%d", seq)
		} else {
			msg.Nak()
			log.Printf("[C]   수신·NAK(미ack): stream_seq=%d", seq)
		}
	}
	if err := msgs.Error(); err != nil {
		log.Printf("[C] msgs error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	info, _ = consumer.Info(ctx)
	log.Printf("[C] 후 AckFloor: stream=%d (기대: 3)", info.AckFloor.Stream)
	log.Printf("[C] NumAckPending=%d (기대: 2 — seq 4,5)", info.NumAckPending)

	log.Println("[C] '연결 끊음' — 3초 대기")
	time.Sleep(3 * time.Second)

	reconnected, err := stream.Consumer(ctx, consumerName)
	if err != nil {
		log.Fatalf("[C] 재접속 실패: %v", err)
	}

	info2, _ := reconnected.Info(ctx)
	log.Printf("[C] 재접속 후 AckFloor: stream=%d (기대: 3)", info2.AckFloor.Stream)
	log.Printf("[C] 재접속 후 NumAckPending=%d (기대: 2)", info2.NumAckPending)

	log.Println("[C] 재접속 후 메세지 수신 시작...")
	var received []uint64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		batch, err := reconnected.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			break
		}
		got := false
		for msg := range batch.Messages() {
			meta, _ := msg.Metadata()
			received = append(received, meta.Sequence.Stream)
			log.Printf("[C]   재수신: stream_seq=%d, payload=%s", meta.Sequence.Stream, string(msg.Data()))
			msg.Ack()
			got = true
		}
		if !got {
			break
		}
	}

	log.Println("[C] ★ Phase C 결과 ★")
	if len(received) == 0 {
		log.Println("[C]   수신된 메세지 없음")
		return
	}
	firstSeq := received[0]
	if firstSeq == 4 {
		log.Printf("[C]   첫 수신 seq: %d → ✓ AckFloor(3)+1=4. 1~3 재수신 없음!", firstSeq)
	} else {
		log.Printf("[C]   첫 수신 seq: %d → 기대(4)와 다름", firstSeq)
	}
	hasDup := false
	for _, s := range received {
		if s <= 3 {
			hasDup = true
			log.Printf("[C]   ✗ seq=%d 재수신됨 (중복!)", s)
		}
	}
	if !hasDup {
		log.Println("[C]   ✓ 1~3 재수신 없음 (AckFloor=3이 막음)")
	}
	log.Printf("[C]   수신 seq 목록: %v", received)
}

// phaseD is the control: consumer expires, new consumer uses OptStartSeq=1 as the only reference.
func phaseD(ctx context.Context, stream jetstream.Stream) {
	log.Println("\n========== Phase D: 대조군 — 컨슈머 만료 후 재생성 ==========")
	shortThreshold := 5 * time.Second
	log.Printf("시나리오: InactiveThreshold=%s 컨슈머 → seq 1~5 ack → 임계값 초과 대기 → 자동 삭제 → 재생성(OptStartSeq=1)", shortThreshold)

	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: shortThreshold,
	})
	if err != nil {
		log.Fatalf("[D] 컨슈머 생성 실패: %v", err)
	}

	info, _ := consumer.Info(ctx)
	consumerName := info.Name
	log.Printf("[D] 컨슈머 생성됨: name=%s, InactiveThreshold=%s", consumerName, shortThreshold)

	// seq 1~5 수신·ack
	log.Println("[D] seq 1~5 수신 및 ack 중...")
	msgs, err := consumer.Fetch(5, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		log.Fatalf("[D] Fetch 실패: %v", err)
	}
	for msg := range msgs.Messages() {
		meta, _ := msg.Metadata()
		log.Printf("[D]   수신·ack: stream_seq=%d", meta.Sequence.Stream)
		msg.Ack()
	}

	time.Sleep(300 * time.Millisecond)
	info, _ = consumer.Info(ctx)
	log.Printf("[D] ack 후 AckFloor: stream=%d (기대: 5)", info.AckFloor.Stream)

	// InactiveThreshold 초과 대기 → 컨슈머 자동 삭제
	// Fetch를 모두 완료한 뒤 대기해야 InactiveThreshold 타이머가 시작됨
	waitTime := shortThreshold + 6*time.Second
	log.Printf("[D] 연결 끊음 — %s 대기 (InactiveThreshold=%s 초과)", waitTime, shortThreshold)
	waitForConsumerDeletion(ctx, stream, consumerName, waitTime)

	// 신규 컨슈머 생성 (OptStartSeq=1, AckFloor 없음)
	log.Println("[D] 신규 컨슈머 생성 (OptStartSeq=1) — AckFloor 없음")
	newConsumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		log.Fatalf("[D] 신규 컨슈머 생성 실패: %v", err)
	}

	newInfo, _ := newConsumer.Info(ctx)
	log.Printf("[D] 신규 컨슈머: name=%s", newInfo.Name)
	log.Printf("[D]   AckFloor.Stream=%d (기대: 0 — AckFloor 없음)", newInfo.AckFloor.Stream)
	log.Printf("[D]   NumPending=%d (기대: 10)", newInfo.NumPending)

	log.Println("[D] 메세지 수신 시작...")
	var received []uint64
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		batch, err := newConsumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			break
		}
		got := false
		for msg := range batch.Messages() {
			meta, _ := msg.Metadata()
			received = append(received, meta.Sequence.Stream)
			log.Printf("[D]   수신: stream_seq=%d, payload=%s", meta.Sequence.Stream, string(msg.Data()))
			msg.Ack()
			got = true
		}
		if !got {
			break
		}
	}

	log.Println("[D] ★ Phase D 결과 ★")
	if len(received) == 0 {
		log.Println("[D]   수신된 메세지 없음")
		return
	}
	firstSeq := received[0]
	switch {
	case firstSeq == 1:
		log.Printf("[D]   첫 수신 seq: %d → ✓ OptStartSeq(1)가 유일 기준. seq 1~10 재수신 = 중복!", firstSeq)
	default:
		log.Printf("[D]   첫 수신 seq: %d → 기대(1)와 다름", firstSeq)
	}
	log.Printf("[D]   수신 seq 목록: %v", received)
	log.Printf("[D]   수신 총 개수: %d (기대: 10)", len(received))

	hasDupFrom1 := false
	for _, s := range received {
		if s <= 5 {
			hasDupFrom1 = true
		}
	}
	if hasDupFrom1 {
		log.Println("[D]   → ✓ 1~5 재수신 확인 (AckFloor 소멸 후 OptStartSeq 유효)")
	}

	log.Println("[D] 결론: Phase B(재사용=중복없음) vs Phase D(재생성=중복) 대조 완료")
	log.Println("[D]   짧은 끊김 = 컨슈머 재사용 = 서버 AckFloor가 시작점 책임")
	log.Println("[D]   긴 끊김(만료) = 컨슈머 재생성 = 클라이언트가 LastSeqNo+1 제공 필요")
}

func waitForConsumerDeletion(ctx context.Context, stream jetstream.Stream, name string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := stream.Consumer(ctx, name)
		if err != nil {
			log.Printf(">> 컨슈머 '%s' 삭제 확인됨", name)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf(">> 경고: 컨슈머 '%s'가 %s 후에도 아직 존재함", name, timeout)
}

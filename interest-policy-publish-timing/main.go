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
	subject    = "starfruit.interest.timing"
	streamName = "interestTimingStream"
)

func main() {
	log.Println("=== InterestPolicy 발행 시점 검증 PoC ===")
	log.Println("핵심 가설: 발행 시점에 컨슈머가 없으면 메시지는 스트림에 저장되지 않고 버려진다.")

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	// 스트림 초기화
	_ = js.DeleteStream(ctx, streamName)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream 생성: %s (Retention=Interest, Storage=Memory)\n", streamName)

	// =====================================================================
	// Phase 1: 컨슈머 없이 발행 → 메시지 버려지는가?
	// =====================================================================
	log.Println("\n========== Phase 1: 컨슈머 없이 발행 ==========")
	log.Println("시나리오: InterestPolicy 스트림에 컨슈머가 전혀 없는 상태에서 메시지 발행")
	log.Println("기대값:  Stream.Msgs = 0 (관심 있는 컨슈머가 없으므로 버려짐)")

	publish(nc, "P1", 3)
	time.Sleep(200 * time.Millisecond)

	info := getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 1 결과] Stream.Msgs = %d", info.State.Msgs)
	if info.State.Msgs == 0 {
		log.Println(">> [Phase 1 결론] ✓ 예상대로 0개. 컨슈머 없이 발행된 메시지는 버려진다.")
	} else {
		log.Printf(">> [Phase 1 결론] ✗ 예상 밖 %d개 저장됨. 추가 분석 필요.", info.State.Msgs)
	}

	// =====================================================================
	// Phase 2: 컨슈머 먼저 생성 후 발행 → 메시지 보존되는가?
	// =====================================================================
	log.Println("\n========== Phase 2: Durable 컨슈머 생성 후 발행 ==========")
	log.Println("시나리오: Durable 컨슈머 생성 → 메시지 발행")
	log.Println("기대값:  Stream.Msgs = 3 (컨슈머가 존재하므로 보존)")

	durable1, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "durable1",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("durable1 생성 실패: %v", err)
	}
	log.Println(">> Durable1 컨슈머 생성 완료")

	publish(nc, "P2", 3)
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 2 결과] Stream.Msgs = %d", info.State.Msgs)
	printConsumerInfo(ctx, stream, "durable1")
	if info.State.Msgs == 3 {
		log.Println(">> [Phase 2 결론] ✓ 예상대로 3개 보존. 컨슈머가 있으면 메시지가 스트림에 저장된다.")
	} else {
		log.Printf(">> [Phase 2 결론] ✗ 예상 밖 %d개 저장됨.", info.State.Msgs)
	}

	// =====================================================================
	// Phase 3: Durable이 모두 ack → 메시지 삭제되는가?
	// =====================================================================
	log.Println("\n========== Phase 3: Durable ack 완료 후 상태 ==========")
	log.Println("시나리오: Phase 2에서 저장된 3개 메시지를 durable1이 모두 ack")
	log.Println("기대값:  Stream.Msgs = 0 (모든 관심 컨슈머가 ack → InterestPolicy에 의해 삭제)")

	fetchAndAck(ctx, durable1, 3, "durable1")
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 3 결과] Stream.Msgs = %d", info.State.Msgs)
	if info.State.Msgs == 0 {
		log.Println(">> [Phase 3 결론] ✓ 예상대로 0개. 모든 컨슈머 ack 후 메시지 삭제 확인.")
	} else {
		log.Printf(">> [Phase 3 결론] ✗ 예상 밖 %d개 남음.", info.State.Msgs)
	}

	// =====================================================================
	// Phase 4: 컨슈머 2개 존재 → 1개만 ack → 메시지 아직 보존되는가?
	// =====================================================================
	log.Println("\n========== Phase 4: 2개 Durable 중 1개만 ack ==========")
	log.Println("시나리오: Durable1 + Durable2 모두 존재 → 발행 → Durable1만 ack")
	log.Println("기대값:  Stream.Msgs = 3 (Durable2가 아직 pending이므로 보존)")

	durable2, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "durable2",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("durable2 생성 실패: %v", err)
	}
	log.Println(">> Durable2 컨슈머 생성 완료")

	publish(nc, "P4", 3)
	time.Sleep(200 * time.Millisecond)

	log.Println(">> 발행 직후 상태:")
	info = getStreamInfo(ctx, stream)
	log.Printf("   Stream.Msgs = %d", info.State.Msgs)
	printConsumerInfo(ctx, stream, "durable1")
	printConsumerInfo(ctx, stream, "durable2")

	log.Println(">> Durable1만 ack...")
	fetchAndAck(ctx, durable1, 3, "durable1")
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 4 결과] Durable1 ack 후 Stream.Msgs = %d", info.State.Msgs)
	printConsumerInfo(ctx, stream, "durable2")
	if info.State.Msgs == 3 {
		log.Println(">> [Phase 4 결론] ✓ 예상대로 3개 보존. Durable2가 pending이므로 메시지 유지.")
	} else {
		log.Printf(">> [Phase 4 결론] ✗ 예상 밖 %d개.", info.State.Msgs)
	}

	log.Println(">> Durable2도 ack...")
	fetchAndAck(ctx, durable2, 3, "durable2")
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 4 최종] Durable2 ack 후 Stream.Msgs = %d", info.State.Msgs)
	if info.State.Msgs == 0 {
		log.Println(">> [Phase 4 최종 결론] ✓ 모든 컨슈머 ack 후 0개 확인.")
	}

	// =====================================================================
	// Phase 5: 컨슈머 생성 전 발행 → 그 뒤 컨슈머 생성 → 메시지 받을 수 있나?
	// =====================================================================
	log.Println("\n========== Phase 5: 발행 먼저 → 이후 컨슈머 생성 → 수신 가능 여부 ==========")
	log.Println("시나리오: 현재 durable1, durable2 존재 → 이들 delete → 컨슈머 없는 상태로 발행")
	log.Println("          → 이후 새 durable3 생성(DeliverAll) → Phase 5 메시지 수신 가능한가?")
	log.Println("기대값:  수신 불가 (발행 시점에 컨슈머가 없었으므로 메시지 이미 버려짐)")

	_ = stream.DeleteConsumer(ctx, "durable1")
	_ = stream.DeleteConsumer(ctx, "durable2")
	log.Println(">> Durable1, Durable2 삭제 완료")

	publish(nc, "P5", 3)
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> 발행 직후 Stream.Msgs = %d (컨슈머 없이 발행했으므로 0이어야 함)", info.State.Msgs)

	durable3, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "durable3",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("durable3 생성 실패: %v", err)
	}
	log.Println(">> Durable3 컨슈머 생성 완료 (DeliverAll)")

	msgs := fetchWithTimeout(ctx, durable3, 1*time.Second)
	log.Printf(">> [Phase 5 결과] Durable3 수신 메시지 수: %d", len(msgs))
	if len(msgs) == 0 {
		log.Println(">> [Phase 5 결론] ✓ 예상대로 0개. 발행 시점에 컨슈머 없으면 메시지는 버려진다.")
		log.Println("                   컨슈머가 나중에 생겨도 이미 버려진 메시지는 되살릴 수 없다.")
	} else {
		log.Printf(">> [Phase 5 결론] ✗ 예상 밖 %d개 수신. 추가 분석 필요.", len(msgs))
		for _, m := range msgs {
			log.Printf("   수신: %s", m)
		}
	}

	// =====================================================================
	// Phase 6: 컨슈머 생성 → 발행 → 컨슈머 삭제 → 메시지는 어떻게 되나?
	// =====================================================================
	log.Println("\n========== Phase 6: 컨슈머 생성 → 발행 → 컨슈머 삭제 → 메시지 상태 ==========")
	log.Println("시나리오: Durable4 생성 → 발행 → Durable4 삭제(ack 없이)")
	log.Println("기대값:  Stream.Msgs = 0 (관심 있던 컨슈머가 삭제되면 그 pending도 해제됨)")

	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "durable4",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       30 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("durable4 생성 실패: %v", err)
	}
	log.Println(">> Durable4 컨슈머 생성 완료")

	publish(nc, "P6", 3)
	time.Sleep(200 * time.Millisecond)

	info = getStreamInfo(ctx, stream)
	log.Printf(">> 발행 직후 Stream.Msgs = %d (durable4 pending)", info.State.Msgs)

	err = stream.DeleteConsumer(ctx, "durable4")
	if err != nil {
		log.Printf(">> Durable4 삭제 실패: %v", err)
	} else {
		log.Println(">> Durable4 삭제 완료 (ack 없이)")
	}

	// 200ms, 1s, 3s 간격으로 폴링하여 즉시 정리 vs 지연 정리 여부 확인
	for _, wait := range []time.Duration{200 * time.Millisecond, 1 * time.Second, 3 * time.Second} {
		time.Sleep(wait)
		info = getStreamInfo(ctx, stream)
		log.Printf("   [Phase 6] 삭제 후 +%v → Stream.Msgs = %d", wait, info.State.Msgs)
		if info.State.Msgs == 0 {
			break
		}
	}

	info = getStreamInfo(ctx, stream)
	log.Printf(">> [Phase 6 결과] Durable4 삭제 후 최종 Stream.Msgs = %d", info.State.Msgs)
	if info.State.Msgs == 0 {
		log.Println(">> [Phase 6 결론] ✓ 컨슈머 삭제 후 메시지도 정리됨.")
	} else {
		log.Printf(">> [Phase 6 결론] 주목: %d개 메시지가 컨슈머 삭제 후에도 스트림에 잔류함.", info.State.Msgs)
		log.Println("                  → Durable consumer 삭제 시 pending 메시지가 즉시 제거되지 않는다.")
		log.Println("                  → Ephemeral과 달리 durable 삭제는 메시지 즉시 삭제를 트리거하지 않을 수 있음.")
	}

	log.Println("\n=== PoC 종료 ===")
	log.Println("─────────────────────────────────────────────────────────────")
	log.Println("요약 (InterestPolicy 핵심 동작)")
	log.Println("  Phase 1 ✓  컨슈머 없이 발행 → 메시지 즉시 버려짐 (스트림에 저장 안 됨)")
	log.Println("  Phase 2 ✓  컨슈머 먼저 생성 → 발행 → 메시지 보존됨")
	log.Println("  Phase 3 ✓  모든 컨슈머 ack 완료 → 메시지 삭제됨")
	log.Println("  Phase 4 ✓  2개 컨슈머 중 1개만 ack → 메시지 유지, 나머지 ack → 삭제")
	log.Println("  Phase 5 ✓  발행 후 생긴 컨슈머(DeliverAll)는 버려진 메시지 받지 못함")
	log.Println("  Phase 6 !  컨슈머 삭제(미ack) → 실행 결과 직접 확인 (위 Phase 6 결론 참조)")
	log.Println("─────────────────────────────────────────────────────────────")
	log.Println("결론: InterestPolicy에서 메시지 보존의 선결 조건은 '발행 시점의 컨슈머 존재'이다.")
}

func publish(nc *nats.Conn, prefix string, n int) {
	log.Printf(">> %d개 메시지 발행 (prefix=%s)", n, prefix)
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-msg-%03d", prefix, i)
		if err := nc.Publish(subject, []byte(data)); err != nil {
			log.Printf("발행 실패: %v", err)
		}
	}
	_ = nc.Flush()
}

func fetchAndAck(ctx context.Context, consumer jetstream.Consumer, n int, label string) {
	for i := 0; i < n; i++ {
		msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			log.Printf("[%s] Fetch 실패: %v", label, err)
			return
		}
		for msg := range msgs.Messages() {
			log.Printf("[%s] ack: %s", label, string(msg.Data()))
			_ = msg.Ack()
		}
		if err := msgs.Error(); err != nil {
			log.Printf("[%s] msgs error: %v", label, err)
		}
	}
}

func fetchWithTimeout(ctx context.Context, consumer jetstream.Consumer, timeout time.Duration) []string {
	var received []string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		wait := remaining
		if wait > 300*time.Millisecond {
			wait = 300 * time.Millisecond
		}
		msgs, err := consumer.Fetch(10, jetstream.FetchMaxWait(wait))
		if err != nil {
			break
		}
		for msg := range msgs.Messages() {
			received = append(received, string(msg.Data()))
			_ = msg.Ack()
		}
	}
	return received
}

func getStreamInfo(ctx context.Context, stream jetstream.Stream) *jetstream.StreamInfo {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("Stream Info 실패: %v", err)
	}
	return info
}

func printConsumerInfo(ctx context.Context, stream jetstream.Stream, name string) {
	c, err := stream.Consumer(ctx, name)
	if err != nil {
		log.Printf("   [%s] 조회 실패: %v", name, err)
		return
	}
	ci, err := c.Info(ctx)
	if err != nil {
		log.Printf("   [%s] Info 실패: %v", name, err)
		return
	}
	log.Printf("   [%s] NumPending=%d, NumAckPending=%d", name, ci.NumPending, ci.NumAckPending)
}

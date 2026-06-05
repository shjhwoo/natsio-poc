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
	subject    = "starfruit.ephemeral.test"
	streamName = "ephemeralTestStream"
	durableName = "durableConsumer"
)

// inactiveThreshold: ephemeral consumer가 active subscription 없이 이 시간 지나면 NATS가 자동 삭제
const inactiveThreshold = 5 * time.Second

func main() {
	log.Println("=== Ephemeral Late-Join + InterestPolicy PoC ===")
	log.Println("검증 목표: InterestPolicy 스트림에서 durable이 ack한 메세지를,")
	log.Println("           나중에 생긴 ephemeral consumer가 받을 수 있는가?")

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

	// 스트림 초기화 (재실행 시 기존 스트림 삭제 후 재생성)
	_ = js.DeleteStream(ctx, streamName)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.MemoryStorage,
		MaxAge:    2 * time.Minute, // TTL: 2분
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream 생성: %s (Retention=Interest, MaxAge=2m)", streamName)

	// =====================================================================
	// Phase 1: durable만 존재할 때 ack → 이후 ephemeral이 늦게 join
	// =====================================================================
	log.Println("\n========== Phase 1: Durable만 있을 때 ack → 늦게 생긴 Ephemeral ==========")
	log.Println("시나리오: durable consumer만 존재 → 메세지 발행 → durable ack")
	log.Println("          → 이후 ephemeral 생성 → ephemeral이 해당 메세지를 받을 수 있는가?")

	// durable consumer 생성
	durable, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durableName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       10 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("durable consumer 생성 실패: %v", err)
	}
	log.Printf(">> Durable consumer 생성: %s", durableName)

	// 메세지 발행
	publishMessages(nc, "P1", 3)

	// durable이 모두 ack
	log.Println(">> Durable consumer가 3개 메세지 모두 ack 중...")
	consumeAndAck(ctx, durable, 3, "durable")

	log.Println(">> Durable ack 완료. Stream 상태:")
	printStreamState(ctx, stream)
	// InterestPolicy: durable만 있고 다 ack했으므로 메세지 0개여야 함

	// 이제 ephemeral consumer 생성 (DeliverAll - 처음부터 받기 시도)
	log.Println("\n>> Ephemeral consumer 생성 (DeliverAll 정책 - 처음부터 받으려고 시도)")
	ephConsumer1, ephName1, err := createEphemeralConsumer(ctx, stream, jetstream.DeliverAllPolicy)
	if err != nil {
		log.Fatalf("ephemeral consumer 생성 실패: %v", err)
	}
	log.Printf(">> Ephemeral consumer 생성됨: %s", ephName1)

	log.Println(">> Ephemeral이 1초간 메세지 대기...")
	received := consumeWithTimeout(ctx, ephConsumer1, 1*time.Second)
	log.Printf(">> [Phase 1 결과] Ephemeral이 받은 메세지 수: %d", len(received))
	if len(received) == 0 {
		log.Println(">> [Phase 1 결론] ✓ 예상대로 0개. durable이 ack한 메세지는 이미 스트림에서 삭제됨.")
		log.Println("                   나중에 생긴 ephemeral은 해당 메세지를 받을 수 없다.")
	} else {
		log.Printf(">> [Phase 1 결론] ✗ 예상 밖 %d개 수신. 추가 분석 필요.", len(received))
		for _, r := range received {
			log.Printf("     수신: %s", r)
		}
	}

	// =====================================================================
	// Phase 2: ephemeral + durable 모두 존재 → ephemeral 비활성으로 삭제됨
	//          → durable만 ack → 새 ephemeral 생성 → 메세지 받을 수 있는가?
	// =====================================================================
	log.Println("\n========== Phase 2: Ephemeral이 비활성 삭제됨 → 새 Ephemeral Late Join ==========")
	log.Println("시나리오: ephemeral + durable 모두 존재 → 메세지 발행")
	log.Println("          → ephemeral이 비활성(InactiveThreshold 초과)으로 NATS가 자동 삭제")
	log.Println("          → durable이 ack → 새 ephemeral 생성 → 메세지 받을 수 있는가?")

	// 새 ephemeral2 생성 (아직 구독하지 않음 - consumer만 만들고 Consume 안 함)
	_, ephName2, err := createEphemeralConsumerNoSubscribe(ctx, stream)
	if err != nil {
		log.Fatalf("ephemeral2 consumer 생성 실패: %v", err)
	}
	log.Printf(">> Ephemeral2 consumer 생성됨 (구독 없음): %s", ephName2)

	// 메세지 발행
	publishMessages(nc, "P2", 3)
	time.Sleep(500 * time.Millisecond)

	log.Println(">> 발행 직후 Stream 상태 (ephemeral2 pending이 있어야 함):")
	printStreamStateWithEphemeral(ctx, stream, ephName2)

	// ephemeral2가 구독 없이 InactiveThreshold 시간 동안 방치 → NATS가 자동 삭제
	log.Printf(">> Ephemeral2를 구독 없이 방치... (%s 후 NATS가 자동 삭제 예정)", inactiveThreshold)
	waitForEphemeralDeletion(ctx, stream, ephName2, inactiveThreshold+3*time.Second)

	log.Println(">> Ephemeral2 삭제 후 Stream 상태:")
	printStreamState(ctx, stream)
	// ephemeral2가 삭제되었지만 durable은 아직 ack 안 함 → 메세지 여전히 있어야 함

	// durable이 ack
	log.Println("\n>> Durable consumer가 P2 메세지 3개 ack 중...")
	consumeAndAck(ctx, durable, 3, "durable")

	log.Println(">> Durable ack 완료 후 Stream 상태 (ephemeral2 삭제 + durable ack → 메세지 0이어야 함):")
	printStreamState(ctx, stream)

	// 새 ephemeral3 생성 (DeliverAll - 처음부터 받기 시도)
	log.Println("\n>> Ephemeral3 생성 (DeliverAll 정책 - 놓친 메세지 복구 시도)")
	ephConsumer3, ephName3, err := createEphemeralConsumer(ctx, stream, jetstream.DeliverAllPolicy)
	if err != nil {
		log.Fatalf("ephemeral3 consumer 생성 실패: %v", err)
	}
	log.Printf(">> Ephemeral3 생성됨: %s", ephName3)

	log.Println(">> Ephemeral3이 1초간 메세지 대기...")
	received3 := consumeWithTimeout(ctx, ephConsumer3, 1*time.Second)
	log.Printf(">> [Phase 2 결과] Ephemeral3이 받은 메세지 수: %d", len(received3))
	if len(received3) == 0 {
		log.Println(">> [Phase 2 결론] ✓ 예상대로 0개.")
		log.Println("                   - Ephemeral2가 비활성으로 삭제되면 그 consumer의 pending이 해제됨")
		log.Println("                   - Durable도 ack하면 InterestPolicy에 의해 메세지 완전 삭제")
		log.Println("                   - 새 Ephemeral3은 이미 사라진 메세지를 받을 수 없다.")
	} else {
		log.Printf(">> [Phase 2 결론] ✗ 예상 밖 %d개 수신:", len(received3))
		for _, r := range received3 {
			log.Printf("     수신: %s", r)
		}
	}

	// =====================================================================
	// Phase 3 (보너스): Durable이 아직 ack 안 했다면? → 새 Ephemeral이 받을 수 있나?
	// =====================================================================
	log.Println("\n========== Phase 3 (보너스): Durable 미ack 상태에서 새 Ephemeral Join ==========")
	log.Println("시나리오: ephemeral 비활성 삭제 → 메세지가 durable pending으로 스트림에 남아있음")
	log.Println("          → 새 ephemeral(DeliverAll)이 join → durable pending 메세지를 받을 수 있나?")

	// ephemeral4 생성 후 구독 없이 방치
	_, ephName4, err := createEphemeralConsumerNoSubscribe(ctx, stream)
	if err != nil {
		log.Fatalf("ephemeral4 생성 실패: %v", err)
	}
	log.Printf(">> Ephemeral4 생성됨 (구독 없음): %s", ephName4)

	publishMessages(nc, "P3", 3)
	time.Sleep(500 * time.Millisecond)

	log.Println(">> 발행 직후 Stream 상태:")
	printStreamStateWithEphemeral(ctx, stream, ephName4)

	// ephemeral4 삭제 기다림 (durable은 아직 ack 안 함)
	log.Printf(">> Ephemeral4 비활성 삭제 대기 중...")
	waitForEphemeralDeletion(ctx, stream, ephName4, inactiveThreshold+3*time.Second)

	log.Println(">> Ephemeral4 삭제 후 Stream 상태 (durable은 아직 ack 안 함):")
	printStreamState(ctx, stream)

	// 새 ephemeral5 생성 (DeliverAll)
	log.Println("\n>> Ephemeral5 생성 (DeliverAll - durable pending 메세지 수신 시도)")
	ephConsumer5, ephName5, err := createEphemeralConsumer(ctx, stream, jetstream.DeliverAllPolicy)
	if err != nil {
		log.Fatalf("ephemeral5 생성 실패: %v", err)
	}
	log.Printf(">> Ephemeral5 생성됨: %s", ephName5)

	log.Println(">> Ephemeral5이 1초간 메세지 대기...")
	received5 := consumeWithTimeout(ctx, ephConsumer5, 1*time.Second)
	log.Printf(">> [Phase 3 결과] Ephemeral5이 받은 메세지 수: %d", len(received5))
	if len(received5) > 0 {
		log.Println(">> [Phase 3 결론] ✓ 메세지 수신 성공!")
		log.Println("                   - Durable이 아직 ack 안 했으므로 메세지가 스트림에 남아 있었음")
		log.Println("                   - DeliverAll 정책으로 새 Ephemeral이 해당 메세지를 수신 가능")
		for _, r := range received5 {
			log.Printf("     수신: %s", r)
		}
		// 수신한 것들 ack
		for _, r := range received5 {
			_ = r // 이미 ack됨 (consumeWithTimeout 내부에서)
		}
	} else {
		log.Println(">> [Phase 3 결론] ✗ 0개 수신. Ephemeral4 삭제 시 해당 pending이 해제되어 메세지도 삭제된 것으로 보임.")
	}

	// durable P3 메세지 정리
	log.Println("\n>> Durable consumer P3 메세지 ack (정리)...")
	consumeAndAck(ctx, durable, 3, "durable-cleanup")

	log.Println("\n=== PoC 종료 ===")
	log.Println("요약:")
	log.Println("  - InterestPolicy 스트림에서 durable이 ack하면 메세지는 스트림에서 삭제됨")
	log.Println("  - 이후에 생긴 ephemeral은 해당 메세지를 받을 수 없음")
	log.Println("  - Ephemeral이 비활성으로 삭제되면 해당 consumer의 pending도 해제됨")
	log.Println("  - WebSocket 기반 ephemeral은 재연결 시 놓친 메세지를 복구할 수 없음 (별도 설계 필요)")
}

func createEphemeralConsumer(ctx context.Context, stream jetstream.Stream, deliver jetstream.DeliverPolicy) (jetstream.Consumer, string, error) {
	c, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckExplicitPolicy,
		FilterSubject:     subject,
		AckWait:           10 * time.Second,
		DeliverPolicy:     deliver,
		InactiveThreshold: inactiveThreshold,
	})
	if err != nil {
		return nil, "", err
	}
	info, _ := c.Info(ctx)
	return c, info.Name, nil
}

func createEphemeralConsumerNoSubscribe(ctx context.Context, stream jetstream.Stream) (jetstream.Consumer, string, error) {
	c, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:         jetstream.AckExplicitPolicy,
		FilterSubject:     subject,
		AckWait:           10 * time.Second,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: inactiveThreshold,
	})
	if err != nil {
		return nil, "", err
	}
	info, _ := c.Info(ctx)
	return c, info.Name, nil
}

func consumeAndAck(ctx context.Context, consumer jetstream.Consumer, n int, label string) {
	for i := 0; i < n; i++ {
		msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			log.Printf("[%s] Fetch 실패: %v", label, err)
			return
		}
		for msg := range msgs.Messages() {
			log.Printf("[%s] 수신 및 ack: %s", label, string(msg.Data()))
			msg.Ack()
		}
		if err := msgs.Error(); err != nil {
			log.Printf("[%s] msgs error: %v", label, err)
		}
	}
}

func consumeWithTimeout(ctx context.Context, consumer jetstream.Consumer, timeout time.Duration) []string {
	var received []string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msgs, err := consumer.Fetch(10, jetstream.FetchMaxWait(min(remaining, 300*time.Millisecond)))
		if err != nil {
			break
		}
		for msg := range msgs.Messages() {
			data := string(msg.Data())
			received = append(received, data)
			msg.Ack()
		}
	}
	return received
}

func publishMessages(nc *nats.Conn, prefix string, n int) {
	log.Printf(">> %d개 메세지 발행 (prefix=%s)", n, prefix)
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-Event-%03d", prefix, i)
		if err := nc.Publish(subject, []byte(data)); err != nil {
			log.Printf("발행 실패: %v", err)
		}
	}
	nc.Flush()
}

func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ Stream 상태 ─────────────────────────────────")
	log.Printf("│  Stream.Msgs = %d", info.State.Msgs)
	// durable 상태
	c, err := stream.Consumer(ctx, durableName)
	if err == nil {
		ci, err := c.Info(ctx)
		if err == nil {
			log.Printf("│  [durable/%s] NumPending=%d, NumAckPending=%d", durableName, ci.NumPending, ci.NumAckPending)
		}
	}
	log.Printf("└───────────────────────────────────────────────")
}

func printStreamStateWithEphemeral(ctx context.Context, stream jetstream.Stream, ephName string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ Stream 상태 ─────────────────────────────────")
	log.Printf("│  Stream.Msgs = %d", info.State.Msgs)
	c, err := stream.Consumer(ctx, durableName)
	if err == nil {
		ci, _ := c.Info(ctx)
		if ci != nil {
			log.Printf("│  [durable/%s] NumPending=%d, NumAckPending=%d", durableName, ci.NumPending, ci.NumAckPending)
		}
	}
	ec, err := stream.Consumer(ctx, ephName)
	if err == nil {
		eci, _ := ec.Info(ctx)
		if eci != nil {
			log.Printf("│  [ephemeral/%s] NumPending=%d, NumAckPending=%d", ephName, eci.NumPending, eci.NumAckPending)
		}
	} else {
		log.Printf("│  [ephemeral/%s] 조회 실패(이미 삭제됨?): %v", ephName, err)
	}
	log.Printf("└───────────────────────────────────────────────")
}

func waitForEphemeralDeletion(ctx context.Context, stream jetstream.Stream, name string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := stream.Consumer(ctx, name)
		if err != nil {
			log.Printf(">> Ephemeral consumer %s 삭제 확인됨 (%v)", name, err)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Printf(">> waitForEphemeralDeletion 타임아웃: %s가 아직 존재함", name)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

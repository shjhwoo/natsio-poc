package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL      = "nats://localhost:4222"
	streamName   = "events"
	subject      = "events.>"
	streamMaxAge = 5 * time.Second // 스트림 단위 공통 TTL (MaxAge)
)

type ServiceHandle struct {
	name       string
	durable    string
	consumeCtx jetstream.ConsumeContext
	processed  atomic.Int64
}

func main() {
	log.Println("=== JetStream 스트림 MaxAge 자동 삭제 검증 PoC ===")
	log.Printf("streamMaxAge=%s (스트림 단위 공통 TTL — publisher 헤더 불필요)", streamMaxAge)

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

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    streamMaxAge,
	})
	if err != nil {
		log.Fatalf("stream 생성 실패: %v", err)
	}
	log.Printf(">> stream: %s (InterestPolicy, MaxAge=%s)", streamName, streamMaxAge)

	serviceA := startService(ctx, stream, "ServiceA", "ServiceA", []string{"events.payment.>"})
	time.Sleep(500 * time.Millisecond)

	// ========== Phase 1: 정상 상태 ==========
	log.Println("\n========== Phase 1: 정상 상태 - ServiceA 가동 ==========")
	log.Println("ServiceA가 ack → InterestPolicy 만족 → 즉시 삭제 (MaxAge 도달 전)")
	publishEvents(nc, 3)
	waitDrain(ctx, stream, 10*time.Second)
	printStream(ctx, stream)
	log.Printf("ServiceA 처리=%d", serviceA.processed.Load())

	// ========== Phase 2: 서비스 다운 → MaxAge로 자동 삭제 ==========
	log.Println("\n========== Phase 2: 서비스 다운 - MaxAge 도달 시 자동 삭제 검증 ==========")
	stopService(serviceA)
	log.Println(">> ServiceA 정지 (durable은 서버에 남음 → InterestPolicy상 보존 조건은 유지)")

	publishEvents(nc, 3)
	time.Sleep(500 * time.Millisecond)
	log.Println(">> 발행 직후 (소비자 없음 → 적체):")
	printStream(ctx, stream)

	wait := streamMaxAge + 3*time.Second
	log.Printf(">> %s 대기 (MaxAge 도달 대기)", wait)
	time.Sleep(wait)
	log.Println(">> MaxAge 도달 후 (소비자 없이도 자동 정리 기대):")
	printStream(ctx, stream)

	// ========== Phase 3: 서비스 복구 → 정상 흐름 재개 ==========
	log.Println("\n========== Phase 3: ServiceA 복구 - 정상 흐름 재개 ==========")
	serviceA = startService(ctx, stream, "ServiceA", "ServiceA", []string{"events.payment.>"})
	time.Sleep(500 * time.Millisecond)

	publishEvents(nc, 3)
	waitDrain(ctx, stream, 10*time.Second)
	printStream(ctx, stream)
	log.Printf("ServiceA 처리(이번 세션)=%d", serviceA.processed.Load())

	log.Println("\n=== PoC 종료 ===")
}

func startService(ctx context.Context, stream jetstream.Stream, name, durable string, filterSubjects []string) *ServiceHandle {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: filterSubjects,
	})
	if err != nil {
		log.Fatalf("[%s] consumer 생성 실패: %v", name, err)
	}

	h := &ServiceHandle{name: name, durable: durable}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		h.processed.Add(1)
		log.Printf("[%s] 처리: subject=%s data=%s", name, msg.Subject(), string(msg.Data()))
		_ = msg.Ack()
	})
	if err != nil {
		log.Fatalf("[%s] Consume 실패: %v", name, err)
	}
	h.consumeCtx = cc
	log.Printf(">> %s 시작 (durable=%s, filter=%v)", name, durable, filterSubjects)
	return h
}

func stopService(h *ServiceHandle) {
	if h != nil && h.consumeCtx != nil {
		h.consumeCtx.Stop()
		h.consumeCtx = nil
		log.Printf(">> %s 정지", h.name)
	}
}

func publishEvents(nc *nats.Conn, n int) {
	for i := 1; i <= n; i++ {
		subj := fmt.Sprintf("events.payment.%03d", i)
		data := []byte(fmt.Sprintf("payment-event-%d", i))
		if err := nc.Publish(subj, data); err != nil {
			log.Printf("발행 실패: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Printf("Flush 실패: %v", err)
	}
	log.Printf(">> %d개 발행 (헤더 없음 — MaxAge가 적용)", n)
}

func printStream(ctx context.Context, stream jetstream.Stream) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ %s Msgs=%d FirstSeq=%d LastSeq=%d",
		streamName, info.State.Msgs, info.State.FirstSeq, info.State.LastSeq)
	consumer, err := stream.Consumer(ctx, "ServiceA")
	if err != nil {
		log.Printf("└─ ServiceA consumer 없음")
		return
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		log.Printf("└─ ServiceA consumer Info 실패: %v", err)
		return
	}
	log.Printf("└─ ServiceA NumPending=%d NumAckPending=%d", ci.NumPending, ci.NumAckPending)
}

func waitDrain(ctx context.Context, stream jetstream.Stream, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := stream.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf(">> waitDrain 타임아웃 (%s)", timeout)
}

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
	streamName = "multiSubjectSink"

	subj1 = "test.subject1" // A, B, C 모두 구독
	subj2 = "test.subject2" // A, B만 구독
	subj3 = "test.subject3" // C만 구독
)

var serviceConfigs = []struct {
	name           string
	durable        string
	filterSubjects []string
}{
	{"serviceA", "ConsumerA", []string{subj1, subj2}},
	{"serviceB", "ConsumerB", []string{subj1, subj2}},
	{"serviceC", "ConsumerC", []string{subj1, subj3}},
}

type ServiceHandle struct {
	name        string
	durable     string
	consumeCtxs []jetstream.ConsumeContext
}

func main() {
	log.Println("=== JetStream Multi-Subject InterestPolicy PoC ===")
	log.Println("subject1: A, B, C 모두 구독")
	log.Println("subject2: A, B만 구독 (C는 미구독)")
	log.Println("subject3: C만 구독 (A, B는 미구독)")

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
		Subjects:  []string{subj1, subj2, subj3},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream 생성: %s (Retention=Interest, Subjects=[subject1,subject2,subject3])", streamName)
	printStreamState(ctx, stream)

	handles := make(map[string]*ServiceHandle)
	for _, sc := range serviceConfigs {
		handles[sc.name] = startService(ctx, stream, sc.name, sc.durable, sc.filterSubjects)
	}
	time.Sleep(1 * time.Second)

	// ========== Phase 1: 전체 서비스 가동, 모든 subject 발행 ==========
	log.Println("\n========== Phase 1: 전체 서비스 가동, 모든 subject에 발행 ==========")
	log.Println("각 서비스가 자신이 구독하는 subject만 수신 → 모두 ack → Stream.Msgs = 0 예상")
	publishTo(nc, "P1", subj1, 3)
	publishTo(nc, "P1", subj2, 3)
	publishTo(nc, "P1", subj3, 3)
	waitForStreamDrain(ctx, stream, 10*time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 2: serviceC 중단 후 subject2 발행 ==========
	log.Println("\n========== Phase 2: serviceC 중단 후 subject2에 발행 ==========")
	log.Println("subject2는 A, B만 구독 → C가 없어도 A, B ack 시 즉시 삭제 예상 (Stream.Msgs = 0)")
	stopService(handles["serviceC"])
	log.Println(">> serviceC 정지 완료")
	publishTo(nc, "P2", subj2, 3)
	time.Sleep(3 * time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 3: serviceC 중단 상태에서 subject1 발행 ==========
	log.Println("\n========== Phase 3: serviceC 중단 상태에서 subject1에 발행 ==========")
	log.Println("subject1은 A, B, C 모두 구독 → C의 durable consumer가 살아있어 메시지 보존 예상 (Stream.Msgs = 3)")
	publishTo(nc, "P3", subj1, 3)
	time.Sleep(3 * time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 4: serviceC 복구 ==========
	log.Println("\n========== Phase 4: serviceC 복구 → subject1 밀린 메시지 처리 ==========")
	log.Println("C 재시작 → ConsumerC NumPending 소진 → Stream.Msgs = 0 예상")
	handles["serviceC"] = startService(ctx, stream, "serviceC", "ConsumerC", []string{subj1, subj3})
	waitForStreamDrain(ctx, stream, 10*time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 5: subject3 단독 검증 ==========
	log.Println("\n========== Phase 5: subject3 발행 (C만 구독, A·B는 미구독) ==========")
	log.Println("A, B의 consumer filter에 subject3 없음 → C만 ack해도 즉시 삭제 예상 (Stream.Msgs = 0)")
	publishTo(nc, "P5", subj3, 3)
	waitForStreamDrain(ctx, stream, 10*time.Second)
	printStreamState(ctx, stream)

	log.Println("\n=== PoC 종료 ===")
}

func startService(ctx context.Context, stream jetstream.Stream, name, durable string, filterSubjects []string) *ServiceHandle {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: filterSubjects,
		AckWait:        30 * time.Second,
	})
	if err != nil {
		log.Fatalf("[%s] consumer 생성 실패: %v", name, err)
	}

	h := &ServiceHandle{name: name, durable: durable}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		time.Sleep(100 * time.Millisecond)
		log.Printf("[처리] %-10s | subject=%-15s | data=%s", name, msg.Subject(), string(msg.Data()))
		msg.Ack()
	})
	if err != nil {
		log.Fatalf("[%s] consume 실패: %v", name, err)
	}
	h.consumeCtxs = append(h.consumeCtxs, cc)
	log.Printf(">> %s 시작 (durable=%s, filterSubjects=%v)", name, durable, filterSubjects)
	return h
}

func stopService(h *ServiceHandle) {
	for _, cc := range h.consumeCtxs {
		cc.Stop()
	}
	h.consumeCtxs = nil
}

func publishTo(nc *nats.Conn, prefix, subject string, n int) {
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-%s-Event-%03d", prefix, subject, i)
		if err := nc.Publish(subject, []byte(data)); err != nil {
			log.Printf("발행 실패 [%s]: %v", subject, err)
		}
	}
	nc.Flush()
	log.Printf(">> %d개 메시지 발행 → %s", n, subject)
}

func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ Stream 상태 ──────────────────────────────")
	log.Printf("│  Stream.Msgs = %d", info.State.Msgs)
	for _, sc := range serviceConfigs {
		c, err := stream.Consumer(ctx, sc.durable)
		if err != nil {
			log.Printf("│  [%-8s / %-10s] consumer 없음 (정지 중)", sc.name, sc.durable)
			continue
		}
		ci, err := c.Info(ctx)
		if err != nil {
			log.Printf("│  [%-8s / %-10s] info 실패: %v", sc.name, sc.durable, err)
			continue
		}
		log.Printf("│  [%-8s / %-10s] NumPending=%d, NumAckPending=%d",
			sc.name, sc.durable, ci.NumPending, ci.NumAckPending)
	}
	log.Printf("└────────────────────────────────────────────")
}

func waitForStreamDrain(ctx context.Context, stream jetstream.Stream, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := stream.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf(">> waitForStreamDrain 타임아웃 (%s)", timeout)
}

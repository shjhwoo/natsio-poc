package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL             = "nats://localhost:4222"
	subject             = "starfruit.internal.event"
	streamName          = "eventSink"
	instancesPerService = 3
)

var serviceDurables = []struct {
	name    string
	durable string
}{
	{"serviceA", "QueueGroupA"},
	{"serviceB", "QueueGroupB"},
	{"serviceC", "QueueGroupC"},
}

type ServiceHandle struct {
	name        string
	durable     string
	consumeCtxs []jetstream.ConsumeContext
}

func main() {
	log.Println("=== JetStream Fanout + InterestPolicy PoC ===")

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf(">> NATS 재연결 성공: %s", c.ConnectedUrl())
		}),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			log.Printf(">> NATS 연결 끊김: %v", err)
		}),
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
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream 생성: %s (Retention=Interest, Storage=File)", streamName)

	log.Println(">> 시작 시점 stream 상태 (재실행 시 잔존 데이터 확인용):")
	printStreamState(ctx, stream)

	handles := make(map[string]*ServiceHandle)
	for _, sd := range serviceDurables {
		handles[sd.name] = startService(ctx, stream, sd.name, sd.durable)
	}
	time.Sleep(1 * time.Second)

	// ========== Phase 1: Baseline ==========
	log.Println("\n========== Phase 1: Baseline ==========")
	log.Println("모든 서비스 가동 → 5 메시지 발행 → 모든 서비스 ack 후 Stream.Msgs = 0 검증")
	publishMessages(nc, "P1", 5)
	waitForStreamDrain(ctx, stream, 10*time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 2: 부분 ack 후 보존 ==========
	log.Println("\n========== Phase 2: 부분 ack 후 보존 ==========")
	log.Println("serviceC 정지 → 5 메시지 발행 → A/B만 ack → Stream.Msgs = 5 보존 검증")
	stopService(handles["serviceC"])
	log.Println(">> serviceC 정지 완료")
	publishMessages(nc, "P2", 5)
	time.Sleep(3 * time.Second) // A/B 처리 대기
	printStreamState(ctx, stream)

	// ========== Phase 3: 복구 후 자동 삭제 ==========
	log.Println("\n========== Phase 3: 복구 후 자동 삭제 ==========")
	log.Println("serviceC 재시작 → 밀린 5개 처리 → 모든 ack 완료 → Stream.Msgs = 0 검증")
	handles["serviceC"] = startService(ctx, stream, "serviceC", "QueueGroupC")
	waitForStreamDrain(ctx, stream, 10*time.Second)
	printStreamState(ctx, stream)

	// ========== Phase 4: 서버 재시작 영속성 ==========
	log.Println("\n========== Phase 4: 서버 재시작 영속성 ==========")
	log.Println("모든 서비스 정지 → 5 메시지 발행 → docker restart → 메시지 보존 검증")
	for _, sd := range serviceDurables {
		stopService(handles[sd.name])
	}
	log.Println(">> 모든 서비스 정지 완료")
	publishMessages(nc, "P4", 5)
	time.Sleep(1 * time.Second)
	log.Println(">> 재시작 직전 stream 상태:")
	printStreamState(ctx, stream)

	log.Println("\n>>> 현재 터미널은 그대로 두고, 별도 터미널에서 아래 명령 실행 후 Enter를 누르세요:")
	log.Println(">>>   docker compose restart nats-hub-server-1")
	log.Println(">>>   (구버전 docker는: docker-compose restart nats-hub-server-1)")
	bufio.NewReader(os.Stdin).ReadBytes('\n')

	log.Println(">> 재연결 대기 중...")
	waitReconnect(nc, 30*time.Second)

	stream, err = waitForStream(ctx, js, streamName, 15*time.Second)
	if err != nil {
		log.Fatalf("재연결 후 스트림 조회 실패: %v", err)
	}
	log.Println(">> 재시작 직후 stream 상태 (메시지가 보존되어야 함):")
	printStreamState(ctx, stream)

	log.Println(">> 모든 서비스 재시작 → 메시지 처리...")
	for _, sd := range serviceDurables {
		handles[sd.name] = startService(ctx, stream, sd.name, sd.durable)
	}
	waitForStreamDrain(ctx, stream, 15*time.Second)
	log.Println(">> 처리 완료 후 stream 상태:")
	printStreamState(ctx, stream)

	log.Println("\n=== PoC 종료 ===")
}

func startService(ctx context.Context, stream jetstream.Stream, name, durable string) *ServiceHandle {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("[%s] consumer 생성 실패: %v", name, err)
	}

	h := &ServiceHandle{name: name, durable: durable}
	for i := 1; i <= instancesPerService; i++ {
		instanceID := i
		cc, err := consumer.Consume(func(msg jetstream.Msg) {
			time.Sleep(100 * time.Millisecond)
			log.Printf("[처리] %s-%d | durable=%s | data=%s", name, instanceID, durable, string(msg.Data()))
			msg.Ack()
		})
		if err != nil {
			log.Fatalf("[%s-%d] consume 실패: %v", name, instanceID, err)
		}
		h.consumeCtxs = append(h.consumeCtxs, cc)
	}
	log.Printf(">> %s 시작 (durable=%s, instances=%d)", name, durable, instancesPerService)
	return h
}

func stopService(h *ServiceHandle) {
	for _, cc := range h.consumeCtxs {
		cc.Stop()
	}
	h.consumeCtxs = nil
}

func publishMessages(nc *nats.Conn, prefix string, n int) {
	log.Printf(">> %d개 메시지 발행 (prefix=%s)", n, prefix)
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
	log.Printf("┌─ Stream 상태 ──────────────────────────────")
	log.Printf("│  Stream.Msgs = %d", info.State.Msgs)
	for _, sd := range serviceDurables {
		c, err := stream.Consumer(ctx, sd.durable)
		if err != nil {
			log.Printf("│  [%s/%s] consumer 조회 실패: %v", sd.name, sd.durable, err)
			continue
		}
		ci, err := c.Info(ctx)
		if err != nil {
			log.Printf("│  [%s/%s] info 실패: %v", sd.name, sd.durable, err)
			continue
		}
		log.Printf("│  [%s/%s] NumPending=%d, NumAckPending=%d",
			sd.name, sd.durable, ci.NumPending, ci.NumAckPending)
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

func waitForStream(ctx context.Context, js jetstream.JetStream, name string, timeout time.Duration) (jetstream.Stream, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		s, err := js.Stream(ctx, name)
		if err == nil {
			return s, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, lastErr
}

func waitReconnect(nc *nats.Conn, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nc.IsConnected() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf(">> waitReconnect 타임아웃")
}

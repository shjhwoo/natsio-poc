package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL       = "nats://localhost:4222"
	streamName    = "multiSubjectSink"
	instanceCount = 2

	subj1 = "test.subject1" // A, B, C 모두 구독
	subj2 = "test.subject2" // A, B만 구독
	subj3 = "test.subject3" // C만 구독
)

const msgsPerSubject = 10

var serviceConfigs = []struct {
	name           string
	durable        string
	filterSubjects []string
}{
	{"serviceA", "ConsumerA", []string{subj1, subj2}},
	{"serviceB", "ConsumerB", []string{subj1, subj2}},
	{"serviceC", "ConsumerC", []string{subj1, subj3}},
}

// InstanceHandle: 서비스의 단일 인스턴스
type InstanceHandle struct {
	id         string
	consumeCtx jetstream.ConsumeContext
	count      atomic.Int64
}

// ServiceHandle: 하나의 durable consumer를 공유하는 서비스 (여러 인스턴스)
type ServiceHandle struct {
	name      string
	durable   string
	instances []*InstanceHandle

	mu         sync.Mutex
	processed  map[uint64]string // streamSeq -> instanceID (중복 처리 감지)
	duplicates int
}

func (sh *ServiceHandle) recordAndCheck(seq uint64, instanceID string) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if prev, exists := sh.processed[seq]; exists {
		sh.duplicates++
		log.Printf("[중복경고!!] %s: seq=%d → 이미 '%s'가 처리했는데 '%s'도 처리 시도!", sh.name, seq, prev, instanceID)
		return false
	}
	sh.processed[seq] = instanceID
	return true
}

func (sh *ServiceHandle) stats() (total int, dups int) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return len(sh.processed), sh.duplicates
}

func makeHandler(inst *InstanceHandle, sh *ServiceHandle) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			log.Printf("[%s] Metadata 조회 실패: %v", inst.id, err)
			_ = msg.Nak()
			return
		}

		seq := meta.Sequence.Stream
		if !sh.recordAndCheck(seq, inst.id) {
			// 중복이지만 일단 ack (재전송 방지)
			_ = msg.Ack()
			return
		}

		time.Sleep(50 * time.Millisecond) // 처리 시뮬레이션
		inst.count.Add(1)
		log.Printf("[처리] %-22s | subj=%-15s | seq=%4d | %s",
			inst.id, msg.Subject(), seq, string(msg.Data()))

		if err := msg.Ack(); err != nil {
			log.Printf("[%s] Ack 실패: %v", inst.id, err)
		}
	}
}

func startService(ctx context.Context, stream jetstream.Stream, name, durable string, filterSubjects []string) *ServiceHandle {
	sh := &ServiceHandle{
		name:      name,
		durable:   durable,
		processed: make(map[uint64]string),
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: filterSubjects,
		AckWait:        30 * time.Second,
	})
	if err != nil {
		log.Fatalf("[%s] consumer 생성 실패: %v", name, err)
	}

	for i := 1; i <= instanceCount; i++ {
		inst := attachInstance(ctx, consumer, fmt.Sprintf("%s-inst%d", name, i), sh)
		sh.instances = append(sh.instances, inst)
	}
	return sh
}

func attachInstance(ctx context.Context, consumer jetstream.Consumer, id string, sh *ServiceHandle) *InstanceHandle {
	inst := &InstanceHandle{id: id}
	cc, err := consumer.Consume(makeHandler(inst, sh))
	if err != nil {
		log.Fatalf("[%s] Consume 시작 실패: %v", id, err)
	}
	inst.consumeCtx = cc
	log.Printf(">> 인스턴스 시작: %s", id)
	return inst
}

func stopInstance(inst *InstanceHandle) {
	inst.consumeCtx.Stop()
	log.Printf(">> 인스턴스 정지: %s (처리 건수: %d)", inst.id, inst.count.Load())
}

func publishTo(nc *nats.Conn, prefix, subject string, n int) {
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-%s-Event-%03d", prefix, subject, i)
		if err := nc.Publish(subject, []byte(data)); err != nil {
			log.Printf("발행 실패 [%s]: %v", subject, err)
		}
	}
	if err := nc.Flush(); err != nil {
		log.Printf("Flush 실패: %v", err)
	}
	log.Printf(">> %d개 메시지 발행 → %s", n, subject)
}

func printDistributionStats(phase string, handles map[string]*ServiceHandle) {
	log.Printf("\n──── [%s] 분산 처리 통계 ────────────────────────────────────", phase)
	for _, sc := range serviceConfigs {
		sh := handles[sc.name]
		total, dups := sh.stats()

		log.Printf("  [%s]", sh.name)
		for _, inst := range sh.instances {
			log.Printf("    %-25s : %d건", inst.id, inst.count.Load())
		}
		log.Printf("    총 고유 메시지 처리 : %d건 | 중복 : %d건", total, dups)
		if dups == 0 {
			log.Printf("    결과 : OK - 중복 처리 없음")
		} else {
			log.Printf("    결과 : FAIL - 중복 처리 %d건 발생!", dups)
		}
	}
	log.Println("────────────────────────────────────────────────────────────")
}

func printStreamState(ctx context.Context, stream jetstream.Stream) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ Stream 상태")
	log.Printf("│  Stream.Msgs = %d", info.State.Msgs)
	for _, sc := range serviceConfigs {
		c, err := stream.Consumer(ctx, sc.durable)
		if err != nil {
			log.Printf("│  [%-8s / %-10s] consumer 없음", sc.name, sc.durable)
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
	log.Println("└────────────────────────────────────────────────────────────")
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

func main() {
	log.Println("=== JetStream Competing Consumers (분산 처리) 검증 PoC ===")
	log.Printf("서비스당 인스턴스 수: %d", instanceCount)
	log.Println("subject1: A, B, C 모두 구독")
	log.Println("subject2: A, B만 구독")
	log.Println("subject3: C만 구독")

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
	log.Printf(">> Stream 생성: %s (Retention=Interest)", streamName)

	handles := make(map[string]*ServiceHandle)
	for _, sc := range serviceConfigs {
		handles[sc.name] = startService(ctx, stream, sc.name, sc.durable, sc.filterSubjects)
	}
	time.Sleep(500 * time.Millisecond)

	// ========== Phase 1: 정상 분산 처리 검증 ==========
	log.Printf("\n========== Phase 1: 정상 상태 - 인스턴스 %d개씩 가동 중 ==========", instanceCount)
	log.Printf("각 서비스의 %d개 인스턴스가 메시지를 분산 처리하는지 검증", instanceCount)
	publishTo(nc, "P1", subj1, msgsPerSubject)
	publishTo(nc, "P1", subj2, msgsPerSubject)
	publishTo(nc, "P1", subj3, msgsPerSubject)
	waitForStreamDrain(ctx, stream, 15*time.Second)
	printStreamState(ctx, stream)
	printDistributionStats("Phase1", handles)

	// ========== Phase 2: 인스턴스 1개 장애 시뮬레이션 ==========
	log.Println("\n========== Phase 2: serviceA inst1 장애 → inst2 혼자 처리 ==========")
	log.Println("inst1 정지 후 메시지 발행 → inst2가 단독으로 모두 처리해야 함")
	stoppedInst := handles["serviceA"].instances[0]
	stopInstance(stoppedInst)
	time.Sleep(200 * time.Millisecond)

	publishTo(nc, "P2", subj1, msgsPerSubject)
	publishTo(nc, "P2", subj2, msgsPerSubject)
	waitForStreamDrain(ctx, stream, 15*time.Second)
	printStreamState(ctx, stream)
	printDistributionStats("Phase2", handles)

	// ========== Phase 3: 장애 인스턴스 복구 ==========
	log.Println("\n========== Phase 3: serviceA inst1 복구 → 다시 분산 처리 ==========")

	sc := serviceConfigs[0] // serviceA
	recoveredConsumer, err := stream.Consumer(ctx, sc.durable)
	if err != nil {
		log.Fatalf("consumer 조회 실패: %v", err)
	}
	recoveredInst := attachInstance(ctx, recoveredConsumer, "serviceA-inst1-recovered", handles["serviceA"])
	handles["serviceA"].instances[0] = recoveredInst
	time.Sleep(200 * time.Millisecond)

	publishTo(nc, "P3", subj1, msgsPerSubject)
	publishTo(nc, "P3", subj2, msgsPerSubject)
	waitForStreamDrain(ctx, stream, 15*time.Second)
	printStreamState(ctx, stream)
	printDistributionStats("Phase3", handles)

	// ========== 최종 결과 ==========
	log.Println("\n========== 최종 검증 결과 ==========")
	allPass := true
	for _, sc := range serviceConfigs {
		_, dups := handles[sc.name].stats()
		if dups > 0 {
			allPass = false
			log.Printf("[FAIL] %s: 중복 처리 %d건", sc.name, dups)
		} else {
			log.Printf("[PASS] %s: 중복 처리 없음", sc.name)
		}
	}
	if allPass {
		log.Println(">> 모든 서비스에서 Competing Consumers 정상 동작 확인")
	} else {
		log.Println(">> 일부 서비스에서 중복 처리 발생 - 확인 필요")
	}
}

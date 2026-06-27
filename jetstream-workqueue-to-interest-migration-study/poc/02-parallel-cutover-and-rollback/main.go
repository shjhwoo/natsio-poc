package main

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL               = "nats://127.0.0.1:4322"
	oldStreamName         = "DATASINK_OLD"
	newLateStreamName     = "DATASINK_NEW_LATE"
	newSafeStreamName     = "DATASINK_NEW_SAFE"
	oldSubjectPattern     = "migration.old.>"
	newLateSubjectPattern = "migration.newlate.>"
	newSafeSubjectPattern = "migration.newsafe.>"
	oldPublishSubject     = "migration.old.event"
	newLatePublishSubject = "migration.newlate.event"
	newSafePublishSubject = "migration.newsafe.event"
	oldConsumerName       = "DATASINK_OLD"
	newLateConsumerName   = "DATASINK_NEW_LATE"
	newSafeConsumerName   = "DATASINK_NEW_SAFE"
)

type Delivery struct {
	Subject   string
	EventID   string
	StreamSeq uint64
	Payload   string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx := context.Background()

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	log.Println("=== WorkQueue -> Interest 병행 이전/컷오버/롤백 PoC ===")
	log.Printf("NATS server version: %s", nc.ConnectedServerVersion())
	log.Printf("nats.go version: %s", moduleVersion("github.com/nats-io/nats.go"))

	oldStream, newLateStream, newSafeStream := setupStreams(ctx, js)
	oldConsumer := mustCreateOrUpdateConsumer(ctx, oldStream, consumerConfig(oldConsumerName))
	newSafeShadow := mustCreateOrUpdateConsumer(ctx, newSafeStream, consumerConfig(newSafeConsumerName))

	phaseASeedBacklog(ctx, js, oldStream, newLateStream, newSafeStream, oldConsumer, newSafeShadow)
	phaseBLateConsumerFailure(ctx, newLateStream)
	phaseCSafeCutover(ctx, js, oldStream, newSafeStream, oldConsumer, newSafeShadow)
	phaseDRollback(ctx, js, oldStream, newSafeStream, oldConsumer)

	log.Println("=== PoC 완료 ===")
}

func setupStreams(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, jetstream.Stream, jetstream.Stream) {
	for _, name := range []string{oldStreamName, newLateStreamName, newSafeStreamName} {
		mustDeleteStream(ctx, js, name)
	}

	oldStream := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:      oldStreamName,
		Subjects:  []string{oldSubjectPattern},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  3,
	})
	newLateStream := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:      newLateStreamName,
		Subjects:  []string{newLateSubjectPattern},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  3,
	})
	newSafeStream := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:      newSafeStreamName,
		Subjects:  []string{newSafeSubjectPattern},
		Retention: jetstream.InterestPolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  3,
	})
	return oldStream, newLateStream, newSafeStream
}

func phaseASeedBacklog(ctx context.Context, js jetstream.JetStream, oldStream, newLateStream, newSafeStream jetstream.Stream, oldConsumer, newSafeShadow jetstream.Consumer) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase A - 야간 이전 준비: old WorkQueue 유지 + new Interest 병행 생성 + safe shadow consumer 선생성")
	log.Println("------------------------------------------------------------")

	publishRange(ctx, js, 1, 6)
	logStreamState(ctx, oldStream, "old backlog 적재 후")
	logStreamState(ctx, newLateStream, "new-late backlog 적재 후")
	logStreamState(ctx, newSafeStream, "new-safe backlog 적재 후")
	logConsumerState(ctx, oldConsumer, "old consumer 초기 상태")
	logConsumerState(ctx, newSafeShadow, "new-safe shadow 초기 상태")

	acked := fetchDeliveries(ctx, oldConsumer, 3, true, "old-ack-1to3")
	log.Printf("old consumer가 먼저 처리한 이벤트(ack): %s", formatDeliveries(acked))
	logStreamState(ctx, oldStream, "old consumer 3개 ack 후")
	logConsumerState(ctx, oldConsumer, "old consumer 3개 ack 후")
}

func phaseBLateConsumerFailure(ctx context.Context, newLateStream jetstream.Stream) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase B - 위험 시나리오: Interest stream에 consumer를 늦게 붙이면 bootstrap backlog가 남는가")
	log.Println("------------------------------------------------------------")

	newLateConsumer := mustCreateOrUpdateConsumer(ctx, newLateStream, consumerConfig(newLateConsumerName))
	logConsumerState(ctx, newLateConsumer, "new-late consumer 생성 직후")
	late := fetchDeliveries(ctx, newLateConsumer, 6, true, "new-late-bootstrap")
	log.Printf("new-late consumer bootstrap 수신: %s", formatDeliveries(late))
	logConsumerState(ctx, newLateConsumer, "new-late bootstrap 후")
	log.Println("결론: InterestPolicy는 관심 consumer가 없던 시점의 메시지를 보존하지 않는다. dual-write를 시작하기 전에 shadow consumer를 미리 만들어야 한다.")
}

func phaseCSafeCutover(ctx context.Context, js jetstream.JetStream, oldStream, newSafeStream jetstream.Stream, oldConsumer, newSafeShadow jetstream.Consumer) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase C - 안전 시나리오: shadow consumer를 미리 만든 상태에서 야간 컷오버")
	log.Println("------------------------------------------------------------")

	bootstrap := fetchDeliveries(ctx, newSafeShadow, 6, true, "new-safe-bootstrap")
	log.Printf("new-safe shadow bootstrap 수신: %s", formatDeliveries(bootstrap))
	logConsumerState(ctx, newSafeShadow, "new-safe bootstrap 후")

	publishRange(ctx, js, 7, 9)
	logStreamState(ctx, oldStream, "e007-e009 dual-write 후 old")
	logStreamState(ctx, newSafeStream, "e007-e009 dual-write 후 new-safe")

	newGot := fetchDeliveries(ctx, newSafeShadow, 3, true, "new-safe-cutover")
	log.Printf("컷오버 후 new-safe consumer 수신: %s", formatDeliveries(newGot))

	oldShadow := fetchDeliveries(ctx, oldConsumer, 3, true, "old-shadow")
	log.Printf("동일 기간 old consumer shadow 수신: %s", formatDeliveries(oldShadow))
	log.Printf("중복 event_id (dual-write overlap 구간): %v", intersectEventIDs(newGot, oldShadow))
	log.Println("결론: safe shadow consumer를 미리 만들어두면 새 Interest 쪽에서도 backlog + 신규 이벤트를 모두 확인하면서 밤 시간대에 천천히 cutover 할 수 있다.")
}

func phaseDRollback(ctx context.Context, js jetstream.JetStream, oldStream, newSafeStream jetstream.Stream, oldConsumer jetstream.Consumer) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase D - 롤백: cutover 후 새 consumer 문제 발생 시 old WorkQueue로 되돌릴 수 있는가")
	log.Println("------------------------------------------------------------")

	publishRange(ctx, js, 10, 11)
	logStreamState(ctx, oldStream, "e010-e011 dual-write 후 old")
	logStreamState(ctx, newSafeStream, "e010-e011 dual-write 후 new-safe")

	rollback := fetchDeliveries(ctx, oldConsumer, 2, true, "old-rollback")
	log.Printf("롤백 후 old consumer가 계속 처리 가능한 이벤트: %s", formatDeliveries(rollback))
	logConsumerState(ctx, oldConsumer, "old consumer rollback 후")
	log.Println("결론: cutover 후에도 dual-write를 유지하고 old stream을 삭제하지 않았다면 old WorkQueue로 롤백 가능하다.")
}

func publishRange(ctx context.Context, js jetstream.JetStream, start, end int) {
	for i := start; i <= end; i++ {
		eventID := fmt.Sprintf("evt-%03d", i)
		payload := fmt.Sprintf("order-%03d", i)
		publishEvent(ctx, js, oldPublishSubject, eventID, payload)
		publishEvent(ctx, js, newLatePublishSubject, eventID, payload)
		publishEvent(ctx, js, newSafePublishSubject, eventID, payload)
	}
}

func publishEvent(ctx context.Context, js jetstream.JetStream, subject, eventID, payload string) {
	msg := &nats.Msg{Subject: subject, Data: []byte(payload), Header: nats.Header{}}
	msg.Header.Set("event-id", eventID)
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		log.Fatalf("publish 실패 %s %s: %v", subject, eventID, err)
	}
	log.Printf("publish subject=%s event_id=%s payload=%s seq=%d", subject, eventID, payload, ack.Sequence)
}

func consumerConfig(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       name,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: -1,
		MaxDeliver:    5,
	}
}

func mustCreateStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) jetstream.Stream {
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		log.Fatalf("stream 생성 실패 (%s): %v", cfg.Name, err)
	}
	log.Printf("stream 생성: %s retention=%s subjects=%v replicas=%d", cfg.Name, retentionLabel(cfg.Retention), cfg.Subjects, cfg.Replicas)
	return stream
}

func mustDeleteStream(ctx context.Context, js jetstream.JetStream, name string) {
	_ = js.DeleteStream(ctx, name)
}

func mustCreateOrUpdateConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) jetstream.Consumer {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		log.Fatalf("consumer 생성 실패 (%s): %v", cfg.Durable, err)
	}
	log.Printf("consumer 준비: %s deliver=%s ack=%s", cfg.Durable, deliverPolicyLabel(cfg.DeliverPolicy), ackPolicyLabel(cfg.AckPolicy))
	return consumer
}

func fetchDeliveries(ctx context.Context, consumer jetstream.Consumer, count int, doAck bool, label string) []Delivery {
	batch, err := consumer.Fetch(count, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		log.Fatalf("fetch 실패 (%s): %v", label, err)
	}

	var out []Delivery
	for msg := range batch.Messages() {
		meta, _ := msg.Metadata()
		d := Delivery{
			Subject:   msg.Subject(),
			EventID:   msg.Headers().Get("event-id"),
			StreamSeq: meta.Sequence.Stream,
			Payload:   string(msg.Data()),
		}
		out = append(out, d)
		if doAck {
			if err := msg.Ack(); err != nil {
				log.Fatalf("ack 실패 (%s): %v", label, err)
			}
		}
	}
	return out
}

func intersectEventIDs(a, b []Delivery) []string {
	left := map[string]struct{}{}
	for _, d := range a {
		left[d.EventID] = struct{}{}
	}
	seen := map[string]struct{}{}
	var out []string
	for _, d := range b {
		if _, ok := left[d.EventID]; ok {
			if _, dup := seen[d.EventID]; !dup {
				seen[d.EventID] = struct{}{}
				out = append(out, d.EventID)
			}
		}
	}
	sort.Strings(out)
	return out
}

func formatDeliveries(items []Delivery) string {
	if len(items) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(items))
	for _, d := range items {
		parts = append(parts, fmt.Sprintf("%s(seq=%d,%s)", d.EventID, d.StreamSeq, d.Payload))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func logStreamState(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("stream info 실패 (%s): %v", label, err)
	}
	log.Printf("[%s] stream=%s retention=%s msgs=%d first_seq=%d last_seq=%d consumers=%d", label, info.Config.Name, retentionLabel(info.Config.Retention), info.State.Msgs, info.State.FirstSeq, info.State.LastSeq, info.State.Consumers)
}

func logConsumerState(ctx context.Context, consumer jetstream.Consumer, label string) {
	info, err := consumer.Info(ctx)
	if err != nil {
		log.Fatalf("consumer info 실패 (%s): %v", label, err)
	}
	log.Printf("[%s] consumer=%s num_pending=%d num_ack_pending=%d ack_floor=%d delivered=%d", label, info.Name, info.NumPending, info.NumAckPending, info.AckFloor.Stream, info.Delivered.Stream)
}

func retentionLabel(p jetstream.RetentionPolicy) string {
	switch p {
	case jetstream.LimitsPolicy:
		return "LimitsPolicy"
	case jetstream.InterestPolicy:
		return "InterestPolicy"
	case jetstream.WorkQueuePolicy:
		return "WorkQueuePolicy"
	default:
		return fmt.Sprintf("RetentionPolicy(%d)", int(p))
	}
}

func deliverPolicyLabel(p jetstream.DeliverPolicy) string {
	switch p {
	case jetstream.DeliverAllPolicy:
		return "DeliverAllPolicy"
	case jetstream.DeliverLastPolicy:
		return "DeliverLastPolicy"
	case jetstream.DeliverNewPolicy:
		return "DeliverNewPolicy"
	case jetstream.DeliverByStartSequencePolicy:
		return "DeliverByStartSequencePolicy"
	case jetstream.DeliverByStartTimePolicy:
		return "DeliverByStartTimePolicy"
	case jetstream.DeliverLastPerSubjectPolicy:
		return "DeliverLastPerSubjectPolicy"
	default:
		return fmt.Sprintf("DeliverPolicy(%d)", int(p))
	}
}

func ackPolicyLabel(p jetstream.AckPolicy) string {
	switch p {
	case jetstream.AckNonePolicy:
		return "AckNonePolicy"
	case jetstream.AckAllPolicy:
		return "AckAllPolicy"
	case jetstream.AckExplicitPolicy:
		return "AckExplicitPolicy"
	default:
		return fmt.Sprintf("AckPolicy(%d)", int(p))
	}
}

func moduleVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == path {
			return dep.Version
		}
	}
	return "unknown"
}

package main

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL       = "nats://127.0.0.1:4222"
	streamName    = "DATASINK"
	streamSubject = "migration.datasink.>"
	publishSubj   = "migration.datasink.event"
	durableName   = "DATASINK"
)

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

	log.Println("=== JetStream/Consumer 설정 마이그레이션 PoC ===")
	log.Printf("NATS server version: %s", nc.ConnectedServerVersion())
	log.Printf("nats.go version: %s", moduleVersion("github.com/nats-io/nats.go"))
	log.Println("테스트 목표: WorkQueuePolicy -> InterestPolicy in-place 변경 가능 여부와 강제 재생성 위험 확인")

	phaseAStreamOnlyRetentionChange(ctx, js)
	phaseBRunningWorkQueueWithDurable(ctx, js)
	phaseCForceDeleteRecreate(ctx, js)

	log.Println("=== PoC 완료 ===")
}

func phaseAStreamOnlyRetentionChange(ctx context.Context, js jetstream.JetStream) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase A - consumer 없이 stream retention만 WorkQueue -> Interest 변경 시도")
	log.Println("------------------------------------------------------------")

	name := streamName + "_ONLY"
	subject := "migration.streamonly.>"
	mustDeleteStream(ctx, js, name)

	stream := mustCreateStream(ctx, js, name, subject, jetstream.WorkQueuePolicy)
	publishMany(ctx, js, subject[:len(subject)-1]+"event", 3, "stream-only")
	logStreamState(ctx, stream, "변경 전")

	updatedCfg := baseStreamConfig(name, subject, jetstream.InterestPolicy)
	_, err := js.CreateOrUpdateStream(ctx, updatedCfg)
	if err != nil {
		log.Printf("retention 변경 결과: 실패: %v", err)
	} else {
		log.Printf("retention 변경 결과: 성공")
	}

	stream = mustLookupStream(ctx, js, name)
	logStreamState(ctx, stream, "변경 후")
}

func phaseBRunningWorkQueueWithDurable(ctx context.Context, js jetstream.JetStream) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase B - 운영 중 WorkQueue stream + durable consumer 상태에서 retention 변경 시도")
	log.Println("------------------------------------------------------------")

	mustDeleteStream(ctx, js, streamName)
	stream := mustCreateStream(ctx, js, streamName, streamSubject, jetstream.WorkQueuePolicy)
	consumer := mustCreateOrUpdateConsumer(ctx, stream, baseConsumerConfig())

	publishMany(ctx, js, publishSubj, 6, "baseline")
	logStreamState(ctx, stream, "메시지 발행 후")
	logConsumerState(ctx, consumer, "소비 전")

	acked := fetchBatch(ctx, consumer, 2, true, "ack")
	unacked := fetchBatch(ctx, consumer, 2, false, "no-ack")
	log.Printf("ack 완료 메시지: %v", acked)
	log.Printf("ack 없이 보류한 메시지: %v", unacked)
	logConsumerState(ctx, consumer, "2개 ack + 2개 no-ack 후")

	updatedCfg := baseStreamConfig(streamName, streamSubject, jetstream.InterestPolicy)
	_, err := js.CreateOrUpdateStream(ctx, updatedCfg)
	if err != nil {
		log.Printf("running stream retention 변경 결과: 실패: %v", err)
	} else {
		log.Printf("running stream retention 변경 결과: 성공")
	}

	stream = mustLookupStream(ctx, js, streamName)
	consumer = mustLookupConsumer(ctx, stream, durableName)
	logStreamState(ctx, stream, "변경 시도 후")
	logConsumerState(ctx, consumer, "변경 시도 후")

	_, err = stream.CreateOrUpdateConsumer(ctx, baseConsumerConfig())
	if err != nil {
		log.Printf("기존 consumer CreateOrUpdate 결과: 실패: %v", err)
	} else {
		log.Printf("기존 consumer CreateOrUpdate 결과: 성공 (하지만 stream retention은 그대로 %s)", retentionLabel(stream))
	}
}

func phaseCForceDeleteRecreate(ctx context.Context, js jetstream.JetStream) {
	log.Println("\n------------------------------------------------------------")
	log.Println("Phase C - retention 변경을 위해 delete/recreate를 강제로 하면 어떤 일이 생기는가")
	log.Println("------------------------------------------------------------")

	stream := mustLookupStream(ctx, js, streamName)
	consumer := mustLookupConsumer(ctx, stream, durableName)
	logStreamState(ctx, stream, "delete 직전")
	logConsumerState(ctx, consumer, "delete 직전")

	if err := js.DeleteStream(ctx, streamName); err != nil {
		log.Fatalf("stream 삭제 실패: %v", err)
	}
	log.Printf("stream %s 삭제 완료", streamName)

	stream = mustCreateStream(ctx, js, streamName, streamSubject, jetstream.InterestPolicy)
	consumer = mustCreateOrUpdateConsumer(ctx, stream, baseConsumerConfig())
	logStreamState(ctx, stream, "InterestPolicy로 재생성 직후")
	logConsumerState(ctx, consumer, "consumer 재생성 직후")

	ack := mustPublish(ctx, js, publishSubj, []byte("after-recreate-001"))
	log.Printf("재생성 후 첫 publish seq=%d", ack.Sequence)
	logStreamState(ctx, stream, "재발행 후")

	msgs := fetchBatch(ctx, consumer, 5, true, "post-recreate")
	log.Printf("재생성 후 consumer가 받은 메시지: %v", msgs)
}

func baseStreamConfig(name, subject string, retention jetstream.RetentionPolicy) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: retention,
		Storage:   jetstream.FileStorage,
		Replicas:  3,
	}
}

func baseConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durableName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: -1,
		MaxDeliver:    5,
	}
}

func mustCreateStream(ctx context.Context, js jetstream.JetStream, name, subject string, retention jetstream.RetentionPolicy) jetstream.Stream {
	stream, err := js.CreateOrUpdateStream(ctx, baseStreamConfig(name, subject, retention))
	if err != nil {
		log.Fatalf("stream 생성 실패 (%s): %v", name, err)
	}
	log.Printf("stream 생성: %s retention=%s storage=File replicas=3 subjects=%v", name, retentionLabelFromPolicy(retention), []string{subject})
	return stream
}

func mustLookupStream(ctx context.Context, js jetstream.JetStream, name string) jetstream.Stream {
	stream, err := js.Stream(ctx, name)
	if err != nil {
		log.Fatalf("stream 조회 실패 (%s): %v", name, err)
	}
	return stream
}

func mustDeleteStream(ctx context.Context, js jetstream.JetStream, name string) {
	_ = js.DeleteStream(ctx, name)
}

func mustCreateOrUpdateConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) jetstream.Consumer {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		log.Fatalf("consumer 생성/업데이트 실패 (%s): %v", cfg.Durable, err)
	}
	log.Printf("consumer 준비: durable=%s deliver=%s ack=%s maxAckPending=%d maxDeliver=%d", cfg.Durable, deliverPolicyLabel(cfg.DeliverPolicy), ackPolicyLabel(cfg.AckPolicy), cfg.MaxAckPending, cfg.MaxDeliver)
	return consumer
}

func mustLookupConsumer(ctx context.Context, stream jetstream.Stream, durable string) jetstream.Consumer {
	consumer, err := stream.Consumer(ctx, durable)
	if err != nil {
		log.Fatalf("consumer 조회 실패 (%s): %v", durable, err)
	}
	return consumer
}

func publishMany(ctx context.Context, js jetstream.JetStream, subject string, count int, prefix string) {
	for i := 1; i <= count; i++ {
		body := []byte(fmt.Sprintf("%s-%03d", prefix, i))
		ack := mustPublish(ctx, js, subject, body)
		log.Printf("publish subject=%s body=%s seq=%d", subject, string(body), ack.Sequence)
	}
}

func mustPublish(ctx context.Context, js jetstream.JetStream, subject string, body []byte) *jetstream.PubAck {
	ack, err := js.Publish(ctx, subject, body)
	if err != nil {
		log.Fatalf("publish 실패 (%s): %v", subject, err)
	}
	return ack
}

func fetchBatch(ctx context.Context, consumer jetstream.Consumer, count int, doAck bool, label string) []string {
	batch, err := consumer.Fetch(count, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		log.Fatalf("fetch 실패 (%s): %v", label, err)
	}

	var out []string
	for msg := range batch.Messages() {
		meta, _ := msg.Metadata()
		entry := fmt.Sprintf("stream_seq=%d data=%s", meta.Sequence.Stream, string(msg.Data()))
		out = append(out, entry)
		if doAck {
			if err := msg.Ack(); err != nil {
				log.Fatalf("ack 실패 (%s): %v", label, err)
			}
		}
	}
	return out
}

func logStreamState(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("stream info 실패 (%s): %v", label, err)
	}
	log.Printf("[%s] stream=%s retention=%s msgs=%d first_seq=%d last_seq=%d consumers=%d", label, info.Config.Name, retentionLabelFromPolicy(info.Config.Retention), info.State.Msgs, info.State.FirstSeq, info.State.LastSeq, info.State.Consumers)
}

func logConsumerState(ctx context.Context, consumer jetstream.Consumer, label string) {
	info, err := consumer.Info(ctx)
	if err != nil {
		log.Fatalf("consumer info 실패 (%s): %v", label, err)
	}
	log.Printf("[%s] consumer=%s num_pending=%d num_ack_pending=%d ack_floor=%d delivered=%d", label, info.Name, info.NumPending, info.NumAckPending, info.AckFloor.Stream, info.Delivered.Stream)
}

func retentionLabel(stream jetstream.Stream) string {
	info := stream.CachedInfo()
	if info == nil {
		return "unknown"
	}
	return retentionLabelFromPolicy(info.Config.Retention)
}

func retentionLabelFromPolicy(p jetstream.RetentionPolicy) string {
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

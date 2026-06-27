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
	natsURL       = "nats://127.0.0.1:4422"
	sourceStream  = "SRC"
	targetV1      = "TGT_V1"
	targetV2      = "TGT_V2"
	sourcePattern = "source.events.>"
	targetV1Pat   = "bridge.v1.>"
	targetV2Pat   = "bridge.v2.>"
)

type Delivery struct {
	EventID   string
	Subject   string
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

	log.Println("=== JetStream Republish Destination Live Update PoC ===")
	log.Printf("NATS server version: %s", nc.ConnectedServerVersion())
	log.Printf("nats.go version: %s", moduleVersion("github.com/nats-io/nats.go"))

	src, v1, v2 := setupStreams(ctx, js)

	log.Println("\n------------------------------------------------------------")
	log.Println("Phase A - source stream 을 republish -> bridge.v1.> 로 시작")
	log.Println("------------------------------------------------------------")
	publishRange(ctx, js, "source.events.alpha", 1, 3)
	waitForReplication()
	logStreamState(ctx, src, "source after evt-001~003")
	logStreamState(ctx, v1, "TGT_V1 after evt-001~003")
	logStreamState(ctx, v2, "TGT_V2 after evt-001~003")
	assertDeliveries(ctx, v1, "v1-consumer-a", []string{"evt-001", "evt-002", "evt-003"})
	assertDeliveries(ctx, v2, "v2-consumer-a", []string{})

	log.Println("\n------------------------------------------------------------")
	log.Println("Phase B - 운영 중 CreateOrUpdateStream() 으로 republish destination 을 bridge.v2.> 로 변경")
	log.Println("------------------------------------------------------------")
	src = mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:     sourceStream,
		Subjects: []string{sourcePattern},
		Storage:  jetstream.FileStorage,
		RePublish: &jetstream.RePublish{
			Source:      ">",
			Destination: "bridge.v2.>",
		},
	})
	logStreamState(ctx, src, "source after republish destination update")

	publishRange(ctx, js, "source.events.beta", 4, 5)
	waitForReplication()
	logStreamState(ctx, src, "source after evt-004~005")
	logStreamState(ctx, v1, "TGT_V1 after evt-004~005")
	logStreamState(ctx, v2, "TGT_V2 after evt-004~005")
	assertNoNewDeliveries(ctx, v1, "v1-consumer-b")
	assertDeliveries(ctx, v2, "v2-consumer-b", []string{"evt-004", "evt-005"})

	log.Println("\n------------------------------------------------------------")
	log.Println("최종 결론")
	log.Println("------------------------------------------------------------")
	log.Println("1. 운영 중 CreateOrUpdateStream() 으로 RePublish destination 변경 가능")
	log.Println("2. 변경 이후 신규 유입 메시지는 새 destination 쪽 stream 으로만 republish 됨")
	log.Println("3. 변경 이전에 예전 destination 으로 republish 된 메시지는 소급 이동하지 않음")
}

func setupStreams(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, jetstream.Stream, jetstream.Stream) {
	for _, name := range []string{sourceStream, targetV1, targetV2} {
		_ = js.DeleteStream(ctx, name)
	}

	src := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:     sourceStream,
		Subjects: []string{sourcePattern},
		Storage:  jetstream.FileStorage,
		RePublish: &jetstream.RePublish{
			Source:      ">",
			Destination: "bridge.v1.>",
		},
	})
	v1 := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:     targetV1,
		Subjects: []string{targetV1Pat},
		Storage:  jetstream.FileStorage,
	})
	v2 := mustCreateStream(ctx, js, jetstream.StreamConfig{
		Name:     targetV2,
		Subjects: []string{targetV2Pat},
		Storage:  jetstream.FileStorage,
	})
	return src, v1, v2
}

func publishRange(ctx context.Context, js jetstream.JetStream, subject string, start, end int) {
	for i := start; i <= end; i++ {
		eventID := fmt.Sprintf("evt-%03d", i)
		payload := fmt.Sprintf("payload-%03d", i)
		msg := &nats.Msg{Subject: subject, Data: []byte(payload), Header: nats.Header{}}
		msg.Header.Set("event-id", eventID)
		ack, err := js.PublishMsg(ctx, msg)
		if err != nil {
			log.Fatalf("publish 실패 %s %s: %v", subject, eventID, err)
		}
		log.Printf("publish source-subject=%s event_id=%s payload=%s src-seq=%d", subject, eventID, payload, ack.Sequence)
	}
}

func assertDeliveries(ctx context.Context, stream jetstream.Stream, consumerName string, want []string) {
	consumer := mustCreateConsumer(ctx, stream, consumerName)
	got := fetchDeliveries(ctx, consumer, len(want))
	gotIDs := eventIDs(got)
	if strings.Join(gotIDs, ",") != strings.Join(want, ",") {
		log.Fatalf("consumer=%s expected=%v got=%v", consumerName, want, gotIDs)
	}
	log.Printf("consumer=%s deliveries=%s", consumerName, formatDeliveries(got))
}

func assertNoNewDeliveries(ctx context.Context, stream jetstream.Stream, consumerName string) {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: -1,
	})
	if err != nil {
		log.Fatalf("consumer 생성 실패 (%s): %v", consumerName, err)
	}
	got := fetchDeliveries(ctx, consumer, 1)
	if len(got) != 0 {
		log.Fatalf("consumer=%s expected no new deliveries, got=%s", consumerName, formatDeliveries(got))
	}
	log.Printf("consumer=%s deliveries=[] (old destination 으로 신규 republish 없음 확인)", consumerName)
}

func mustCreateStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) jetstream.Stream {
	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		log.Fatalf("stream 생성/업데이트 실패 (%s): %v", cfg.Name, err)
	}
	log.Printf("stream 준비: %s subjects=%v republish=%s", cfg.Name, cfg.Subjects, republishLabel(cfg.RePublish))
	return stream
}

func mustCreateConsumer(ctx context.Context, stream jetstream.Stream, durable string) jetstream.Consumer {
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: -1,
	})
	if err != nil {
		log.Fatalf("consumer 생성 실패 (%s): %v", durable, err)
	}
	return consumer
}

func fetchDeliveries(ctx context.Context, consumer jetstream.Consumer, count int) []Delivery {
	if count == 0 {
		return nil
	}
	batch, err := consumer.Fetch(count, jetstream.FetchMaxWait(1200*time.Millisecond))
	if err != nil {
		log.Fatalf("fetch 실패: %v", err)
	}
	var out []Delivery
	for msg := range batch.Messages() {
		meta, _ := msg.Metadata()
		out = append(out, Delivery{
			EventID:   msg.Headers().Get("event-id"),
			Subject:   msg.Subject(),
			StreamSeq: meta.Sequence.Stream,
			Payload:   string(msg.Data()),
		})
		if err := msg.Ack(); err != nil {
			log.Fatalf("ack 실패: %v", err)
		}
	}
	return out
}

func logStreamState(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("stream info 실패 (%s): %v", label, err)
	}
	log.Printf("[%s] stream=%s msgs=%d first_seq=%d last_seq=%d republish=%s", label, info.Config.Name, info.State.Msgs, info.State.FirstSeq, info.State.LastSeq, republishLabel(info.Config.RePublish))
}

func republishLabel(r *jetstream.RePublish) string {
	if r == nil {
		return "<none>"
	}
	return fmt.Sprintf("{src=%s dest=%s}", r.Source, r.Destination)
}

func eventIDs(items []Delivery) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.EventID)
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
		parts = append(parts, fmt.Sprintf("%s(seq=%d,subject=%s,payload=%s)", d.EventID, d.StreamSeq, d.Subject, d.Payload))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func waitForReplication() {
	time.Sleep(500 * time.Millisecond)
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

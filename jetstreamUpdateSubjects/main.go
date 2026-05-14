package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL    = "nats://localhost:4222"
	streamName = "subjectUpdateTest"
	durable    = "Worker"

	subjA = "evt.a"
	subjB = "evt.b"
	subjC = "evt.c"
	subjX = "brand.new.x"
	subjY = "brand.new.y"
)

func main() {
	log.Println("=== JetStream CreateOrUpdateStream Subjects 변경 PoC ===")
	log.Println("운영중인 stream의 Subjects를 CreateOrUpdateStream으로 바꿀 수 있는지 검증")

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

	// 이전 실행 잔재 정리 (있으면 삭제)
	_ = js.DeleteStream(ctx, streamName)

	// ========== Phase 0: 초기 스트림/컨슈머 구성 ==========
	log.Println("\n========== Phase 0: 초기 스트림/컨슈머 구성 ==========")
	log.Println("Subjects=[evt.a, evt.b], Retention=Limits, FilterSubjects=[evt.a, evt.b]")
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjA, subjB},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("초기 스트림 생성 실패: %v", err)
	}
	printStream(ctx, stream, "초기 생성 직후")

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subjA, subjB},
		AckWait:        30 * time.Second,
	})
	if err != nil {
		log.Fatalf("초기 컨슈머 생성 실패: %v", err)
	}
	cc := startConsume(consumer)

	publish(ctx, js, subjA, 2)
	publish(ctx, js, subjB, 2)
	time.Sleep(2 * time.Second)
	printStream(ctx, stream, "Phase 0 종료 (정상 동작 확인)")

	// ========== Phase 1: subject 추가 ==========
	log.Println("\n========== Phase 1: subject 추가 ([evt.a, evt.b] → [evt.a, evt.b, evt.c]) ==========")
	updateStreamSubjects(ctx, js, []string{subjA, subjB, subjC})
	printStream(ctx, stream, "Phase 1 update 직후")

	log.Println(">> evt.c 발행 (consumer FilterSubjects에는 아직 없음)")
	publish(ctx, js, subjC, 3)
	time.Sleep(1 * time.Second)
	printStream(ctx, stream, "evt.c 발행 후 / consumer 미필터 상태")

	log.Println(">> Consumer FilterSubjects 갱신: [evt.a, evt.b, evt.c]")
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subjA, subjB, subjC},
		AckWait:        30 * time.Second,
	}); err != nil {
		log.Printf(">> consumer 갱신 실패: %v", err)
	} else {
		log.Printf(">> consumer 갱신 성공")
	}
	time.Sleep(2 * time.Second)
	printStream(ctx, stream, "Phase 1 종료 (consumer가 evt.c 흡수)")

	// ========== Phase 2: subject 제거 (잔존 메시지 있는 상태) ==========
	log.Println("\n========== Phase 2: subject 제거 시나리오 ==========")
	log.Println(">> (2-a) consumer 정지 → evt.a로 5개 발행 → 스트림 내 잔존")
	cc.Stop()
	time.Sleep(500 * time.Millisecond)
	publish(ctx, js, subjA, 5)
	time.Sleep(1 * time.Second)
	printStream(ctx, stream, "evt.a 5개 잔존")

	log.Println(">> (2-b) [evt.b, evt.c]로 update 시도 — evt.a 메시지가 남아있는 상태")
	updateStreamSubjects(ctx, js, []string{subjB, subjC})
	printStream(ctx, stream, "Phase 2-b 직후")

	log.Println(">> (2-c) evt.a 메시지 purge 후 다시 update 시도")
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(subjA)); err != nil {
		log.Printf(">> evt.a purge 실패: %v", err)
	} else {
		log.Printf(">> evt.a purge 성공")
	}
	printStream(ctx, stream, "evt.a purge 후")

	updateStreamSubjects(ctx, js, []string{subjB, subjC})
	printStream(ctx, stream, "Phase 2-c 직후")

	log.Println(">> (2-d) 제거된 evt.a에 publish 시도 — JetStream이 거부하는가?")
	if _, err := js.Publish(ctx, subjA, []byte("ghost")); err != nil {
		log.Printf(">> evt.a publish 거부: %v", err)
	} else {
		log.Printf(">> evt.a publish 성공 (예상 외, core NATS로 흘러 사라졌을 수 있음)")
	}

	// ========== Phase 3: subject 완전 교체 ==========
	log.Println("\n========== Phase 3: subject 완전 교체 ([evt.b, evt.c] → [brand.new.x, brand.new.y]) ==========")
	updateStreamSubjects(ctx, js, []string{subjX, subjY})
	printStream(ctx, stream, "Phase 3 update 직후")

	log.Println(">> 새 subject로 발행")
	publish(ctx, js, subjX, 2)
	publish(ctx, js, subjY, 2)
	time.Sleep(500 * time.Millisecond)
	printStream(ctx, stream, "Phase 3 종료")

	// ========== Phase 4: stale consumer FilterSubjects ==========
	log.Println("\n========== Phase 4: consumer의 FilterSubjects가 더 이상 stream에 없을 때 ==========")
	if ci, err := consumer.Info(ctx); err != nil {
		log.Printf(">> consumer Info 실패: %v", err)
	} else {
		log.Printf(">> 현재 consumer FilterSubjects = %v", ci.Config.FilterSubjects)
		log.Printf(">> stream Subjects = %v", []string{subjX, subjY})
		log.Printf(">> NumPending=%d, NumAckPending=%d", ci.NumPending, ci.NumAckPending)
	}

	log.Println(">> consumer를 새 subject로 갱신 시도")
	if _, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{subjX, subjY},
		AckWait:        30 * time.Second,
	}); err != nil {
		log.Printf(">> consumer 갱신 실패: %v", err)
	} else {
		log.Printf(">> consumer 갱신 성공")
	}

	log.Println("\n=== PoC 종료 ===")
}

func updateStreamSubjects(ctx context.Context, js jetstream.JetStream, subjects []string) {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  subjects,
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) {
			log.Printf(">> CreateOrUpdateStream(%v) 실패: code=%d errCode=%d desc=%q",
				subjects, apiErr.Code, apiErr.ErrorCode, apiErr.Description)
		} else {
			log.Printf(">> CreateOrUpdateStream(%v) 실패: %v", subjects, err)
		}
		return
	}
	log.Printf(">> CreateOrUpdateStream(%v) 성공", subjects)
}

func startConsume(c jetstream.Consumer) jetstream.ConsumeContext {
	cc, err := c.Consume(func(msg jetstream.Msg) {
		log.Printf("[수신] subject=%-15s data=%s", msg.Subject(), string(msg.Data()))
		msg.Ack()
	})
	if err != nil {
		log.Fatalf("consume 실패: %v", err)
	}
	return cc
}

func publish(ctx context.Context, js jetstream.JetStream, subject string, n int) {
	pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-#%03d", subject, i)
		if _, err := js.Publish(pubCtx, subject, []byte(data)); err != nil {
			var apiErr *jetstream.APIError
			if errors.As(err, &apiErr) {
				log.Printf(">> publish[%s] 거부: code=%d desc=%q", subject, apiErr.ErrorCode, apiErr.Description)
			} else {
				log.Printf(">> publish[%s] 실패: %v", subject, err)
			}
			return
		}
	}
	log.Printf(">> %d개 발행 → %s", n, subject)
}

func printStream(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ [%s]", label)
	log.Printf("│  Config.Subjects = %v", info.Config.Subjects)
	log.Printf("│  State.Msgs      = %d   (FirstSeq=%d, LastSeq=%d)",
		info.State.Msgs, info.State.FirstSeq, info.State.LastSeq)
	log.Printf("└──────────────────────────────")
}

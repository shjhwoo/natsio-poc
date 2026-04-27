package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL       = nats.DefaultURL
	streamName    = "WQ_DEMO"
	subjectName   = "jobs.same"
	totalMessages = 12
)

func main() {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("JetStream 컨텍스트 생성 실패: %v", err)
	}

	ctx := context.Background()

	fmt.Println("=== Case 1: 같은 subject에 durable consumer 여러 개 생성 시도 ===")
	if err := resetStream(js); err != nil {
		log.Fatalf("스트림 초기화 실패: %v", err)
	}
	if err := publishTasks(js, "case1"); err != nil {
		log.Fatalf("메시지 발행 실패(case1): %v", err)
	}
	if err := caseMultipleDurables(js, ctx); err != nil {
		log.Fatalf("case1 실패: %v", err)
	}

	// fmt.Println()
	// fmt.Println("=== Case 2: durable consumer 하나 + 여러 worker가 consume ===")
	// if err := resetStream(js); err != nil {
	// 	log.Fatalf("스트림 초기화 실패: %v", err)
	// }
	// if err := publishTasks(js, "case2"); err != nil {
	// 	log.Fatalf("메시지 발행 실패(case2): %v", err)
	// }
	// if err := caseSingleDurableMultipleWorkers(js, ctx); err != nil {
	// 	log.Fatalf("case2 실패: %v", err)
	// }
}

func resetStream(js nats.JetStreamContext) error {
	_ = js.DeleteStream(streamName)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjectName},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.MemoryStorage,
	})
	return err
}

func publishTasks(js nats.JetStreamContext, prefix string) error {
	for i := 1; i <= totalMessages; i++ {
		payload := fmt.Sprintf("%s-task-%02d", prefix, i)
		if _, err := js.Publish(subjectName, []byte(payload)); err != nil {
			return err
		}
	}
	return nil
}

func caseMultipleDurables(js nats.JetStreamContext, ctx context.Context) error {
	_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       "DUR_A",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subjectName,
	})
	if err != nil {
		return fmt.Errorf("첫 durable 생성 실패: %w", err)
	}
	fmt.Println("[OK] DUR_A 생성 성공")

	durBCreated := true
	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       "DUR_B",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subjectName,
	})
	if err != nil {
		durBCreated = false
		fmt.Printf("[EXPECTED] DUR_B 생성 실패(같은 subject 중복): %v\n", err)
	} else {
		fmt.Println("[OK] DUR_B 생성 성공")
	}

	if err := consumeAllFromDurable(js, ctx, "DUR_A"); err != nil {
		return err
	}

	if durBCreated {
		if err := consumeAllFromDurable(js, ctx, "DUR_B"); err != nil {
			return err
		}
	} else {
		fmt.Println("[INFO] DUR_B는 생성되지 않아 consume 불가(WorkQueue 중복 필터 제약)")
	}

	return nil
}

func consumeAllFromDurable(js nats.JetStreamContext, ctx context.Context, durable string) error {
	sub, err := js.PullSubscribe(subjectName, durable, nats.Bind(streamName, durable))
	if err != nil {
		return fmt.Errorf("%s bind 실패: %w", durable, err)
	}

	count := 0
	for {
		msgs, fetchErr := sub.Fetch(5, nats.Context(ctx), nats.MaxWait(300*time.Millisecond))
		if fetchErr != nil {
			if errors.Is(fetchErr, context.DeadlineExceeded) || fetchErr == nats.ErrTimeout {
				break
			}
			return fmt.Errorf("%s fetch 실패: %w", durable, fetchErr)
		}
		for _, msg := range msgs {
			fmt.Printf("%s consumed: %s\n", durable, string(msg.Data))
			_ = msg.Ack()
			count++
		}
	}
	fmt.Printf("[OK] %s consumed count = %d\n", durable, count)
	return nil
}

func caseSingleDurableMultipleWorkers(js nats.JetStreamContext, ctx context.Context) error {
	_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       "DUR_SINGLE",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subjectName,
	})
	if err != nil {
		return fmt.Errorf("single durable 생성 실패: %w", err)
	}

	workerCount := 3
	workerSubs := make([]*nats.Subscription, 0, workerCount)
	workerConsumed := make([]int, workerCount)

	for i := 1; i <= workerCount; i++ {
		sub, subErr := js.PullSubscribe(subjectName, "DUR_SINGLE", nats.Bind(streamName, "DUR_SINGLE"))
		if subErr != nil {
			return fmt.Errorf("worker-%d subscribe 실패: %w", i, subErr)
		}
		workerSubs = append(workerSubs, sub)
	}

	for {
		progressed := false
		for idx, sub := range workerSubs {
			msgs, fetchErr := sub.Fetch(1, nats.Context(ctx), nats.MaxWait(250*time.Millisecond))
			if fetchErr != nil {
				if errors.Is(fetchErr, context.DeadlineExceeded) || fetchErr == nats.ErrTimeout {
					continue
				}
				return fmt.Errorf("worker-%d fetch 실패: %w", idx+1, fetchErr)
			}

			for _, msg := range msgs {
				progressed = true
				workerConsumed[idx]++
				fmt.Printf("worker-%d consumed: %s\n", idx+1, string(msg.Data))
				_ = msg.Ack()
			}
		}

		if !progressed {
			break
		}
	}

	for idx, count := range workerConsumed {
		fmt.Printf("worker-%d consumed count = %d\n", idx+1, count)
	}
	fmt.Println("[OK] 단일 durable(DUR_SINGLE) 기준으로 각 worker consume 확인 완료")
	return nil
}

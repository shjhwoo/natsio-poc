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
	natsURL = "nats://localhost:4222"

	// Case 1: Sources 추가 검증에 쓰이는 스트림 이름
	sourceStreamName = "src-stream"   // Sources의 원본 스트림
	targetStreamName = "tgt-stream"   // Sources를 나중에 추가할 대상 스트림
	sourceSubject    = "src.events.>" // sourceStream이 수집하는 subject
	targetSubject    = "tgt.events.>" // targetStream이 수집하는 subject

	// Case 2: Republish 추가 검증에 쓰이는 스트림 이름
	republishStreamName   = "repub-stream"
	republishSrcSubject   = "repub.in.>"  // 스트림이 수집하는 subject
	republishDestSubject  = "repub.out.>" // Republish 설정 후 메시지가 재발행될 subject
)

func main() {
	log.Println("=== JetStream UpdateStream: Sources / Republish 추가 PoC ===")

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

	runCase1Sources(ctx, js)
	runCase2Republish(ctx, js, nc)
}

// =============================================================================
// Case 1: Sources 없이 생성된 스트림에 Sources를 추가할 수 있는가?
// =============================================================================
func runCase1Sources(ctx context.Context, js jetstream.JetStream) {
	log.Println("\n" + separator("Case 1: Sources 추가 검증"))

	// 잔재 정리
	_ = js.DeleteStream(ctx, sourceStreamName)
	_ = js.DeleteStream(ctx, targetStreamName)

	// Phase 1-a: 원본 스트림(sourceStream) 생성
	log.Println("\n[Phase 1-a] 원본 스트림 생성 (Sources의 소스가 될 스트림)")
	srcStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     sourceStreamName,
		Subjects: []string{sourceSubject},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("sourceStream 생성 실패: %v", err)
	}
	printStreamInfo(ctx, srcStream, "sourceStream 생성 직후")

	// Phase 1-b: 대상 스트림(targetStream) Sources 없이 생성
	log.Println("\n[Phase 1-b] 대상 스트림 생성 — Sources 설정 없음")
	tgtStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     targetStreamName,
		Subjects: []string{targetSubject},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("targetStream 생성 실패: %v", err)
	}
	printStreamInfo(ctx, tgtStream, "targetStream 생성 직후 (Sources 없음)")

	// 초기 메시지 발행
	publish(ctx, js, "src.events.hello", 3)
	publish(ctx, js, "tgt.events.hello", 2)
	time.Sleep(500 * time.Millisecond)
	printStreamInfo(ctx, srcStream, "초기 발행 후 sourceStream")
	printStreamInfo(ctx, tgtStream, "초기 발행 후 targetStream (독립 상태)")

	// Phase 1-c: targetStream에 Sources 추가 시도
	log.Println("\n[Phase 1-c] targetStream에 Sources 추가 시도 (CreateOrUpdateStream)")
	log.Printf("  → Sources: [{Name: %q}]", sourceStreamName)
	updatedTgt, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     targetStreamName,
		Subjects: []string{targetSubject},
		Storage:  jetstream.FileStorage,
		Sources: []*jetstream.StreamSource{
			{Name: sourceStreamName},
		},
	})
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) {
			log.Printf("✗ Sources 추가 실패 (APIError): HTTP=%d ErrCode=%d desc=%q",
				apiErr.Code, apiErr.ErrorCode, apiErr.Description)
		} else {
			log.Printf("✗ Sources 추가 실패: %v", err)
		}
		log.Println("  → 결론: 운영 중인 스트림에 Sources를 사후 추가할 수 없음")
	} else {
		log.Println("✓ Sources 추가 성공")
		printStreamInfo(ctx, updatedTgt, "Sources 추가 후 targetStream")

		// 검증: sourceStream에 새 메시지 발행 → targetStream에서 보이는지 확인
		log.Println("  → sourceStream에 메시지 발행 후 targetStream 미러링 여부 확인")
		publish(ctx, js, "src.events.after-update", 3)
		time.Sleep(1 * time.Second)
		printStreamInfo(ctx, srcStream, "발행 후 sourceStream")
		printStreamInfo(ctx, updatedTgt, "발행 후 targetStream (Sources 경유 메시지 포함 여부)")
	}

	log.Println("\n" + separator("Case 1 종료"))
}

// =============================================================================
// Case 2: Republish 없이 생성된 스트림에 Republish를 추가할 수 있는가?
// =============================================================================
func runCase2Republish(ctx context.Context, js jetstream.JetStream, nc *nats.Conn) {
	log.Println("\n" + separator("Case 2: Republish 추가 검증"))

	// 잔재 정리
	_ = js.DeleteStream(ctx, republishStreamName)

	// Phase 2-a: Republish 없이 스트림 생성
	log.Println("\n[Phase 2-a] 스트림 생성 — Republish 설정 없음")
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     republishStreamName,
		Subjects: []string{republishSrcSubject},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatalf("republishStream 생성 실패: %v", err)
	}
	printStreamInfo(ctx, stream, "생성 직후 (Republish 없음)")

	// repub.out.> 구독 (Republish 검증용 — core NATS 구독)
	received := make(chan string, 20)
	sub, err := nc.Subscribe(republishDestSubject, func(msg *nats.Msg) {
		received <- fmt.Sprintf("subject=%s data=%s", msg.Subject, string(msg.Data))
	})
	if err != nil {
		log.Fatalf("repub.out.> 구독 실패: %v", err)
	}
	defer sub.Unsubscribe()

	// 초기 메시지 발행 (Republish 없는 상태)
	log.Println("\n[Phase 2-b] Republish 추가 전 메시지 발행")
	publish(ctx, js, "repub.in.hello", 2)
	time.Sleep(500 * time.Millisecond)
	drainReceived(received, "Republish 추가 전 repub.out.> 수신 메시지")

	// Phase 2-c: Republish 추가 시도
	log.Println("\n[Phase 2-c] Republish 추가 시도 (CreateOrUpdateStream)")
	log.Printf("  → RePublish: {Source: %q, Destination: %q}", ">", republishDestSubject)
	updatedStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     republishStreamName,
		Subjects: []string{republishSrcSubject},
		Storage:  jetstream.FileStorage,
		RePublish: &jetstream.RePublish{
			Source:      ">",
			Destination: republishDestSubject,
		},
	})
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) {
			log.Printf("✗ Republish 추가 실패 (APIError): HTTP=%d ErrCode=%d desc=%q",
				apiErr.Code, apiErr.ErrorCode, apiErr.Description)
		} else {
			log.Printf("✗ Republish 추가 실패: %v", err)
		}
		log.Println("  → 결론: 운영 중인 스트림에 Republish를 사후 추가할 수 없음")
	} else {
		log.Println("✓ Republish 추가 성공")
		printStreamInfo(ctx, updatedStream, "Republish 추가 후 스트림")

		// 검증: Republish 추가 후 메시지 발행 → repub.out.>에서 수신되는지 확인
		log.Println("\n[Phase 2-d] Republish 추가 후 메시지 발행 → 재발행 여부 확인")
		publish(ctx, js, "repub.in.after-update", 3)
		time.Sleep(1 * time.Second)
		drainReceived(received, "Republish 추가 후 repub.out.> 수신 메시지")
	}

	log.Println("\n" + separator("Case 2 종료"))
}

// =============================================================================
// 유틸
// =============================================================================

func publish(ctx context.Context, js jetstream.JetStream, subject string, n int) {
	pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("%s-#%03d", subject, i)
		if _, err := js.Publish(pubCtx, subject, []byte(data)); err != nil {
			var apiErr *jetstream.APIError
			if errors.As(err, &apiErr) {
				log.Printf("  publish[%s] 거부: ErrCode=%d desc=%q", subject, apiErr.ErrorCode, apiErr.Description)
			} else {
				log.Printf("  publish[%s] 실패: %v", subject, err)
			}
			return
		}
	}
	log.Printf("  >> %d개 발행 → %s", n, subject)
}

func printStreamInfo(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	if err != nil {
		log.Printf("  Stream Info 실패: %v", err)
		return
	}
	log.Printf("┌─ [%s]", label)
	log.Printf("│  Name            = %s", info.Config.Name)
	log.Printf("│  Config.Subjects = %v", info.Config.Subjects)
	log.Printf("│  Config.Sources  = %v", sourcesToStr(info.Config.Sources))
	log.Printf("│  Config.RePublish= %v", republishToStr(info.Config.RePublish))
	log.Printf("│  State.Msgs      = %d  (FirstSeq=%d, LastSeq=%d)",
		info.State.Msgs, info.State.FirstSeq, info.State.LastSeq)
	log.Printf("└──────────────────────────────")
}

func sourcesToStr(sources []*jetstream.StreamSource) string {
	if len(sources) == 0 {
		return "<없음>"
	}
	result := "["
	for i, s := range sources {
		if i > 0 {
			result += ", "
		}
		result += s.Name
	}
	return result + "]"
}

func republishToStr(rp *jetstream.RePublish) string {
	if rp == nil {
		return "<없음>"
	}
	return fmt.Sprintf("{src=%q dest=%q}", rp.Source, rp.Destination)
}

func drainReceived(ch chan string, label string) {
	count := 0
	for {
		select {
		case msg := <-ch:
			log.Printf("  [%s] 수신: %s", label, msg)
			count++
		default:
			if count == 0 {
				log.Printf("  [%s] 수신 메시지 없음", label)
			}
			return
		}
	}
}

func separator(title string) string {
	return fmt.Sprintf("========== %s ==========", title)
}

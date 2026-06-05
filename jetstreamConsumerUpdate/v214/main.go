package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	natsURL    = "nats://localhost:4222"
	streamName = "FILTER_TEST"
	subA       = "test.a"
	subB       = "test.b"
	subC       = "test.c"
	rounds     = 5
)

// 2.14 Consumer Reset API: $JS.API.CONSUMER.RESET.<STREAM>.<CONSUMER>
// 페이로드 {"seq":N} → AckFloor = N-1, 다음 전달 seq=N부터
// 페이로드 nil(빈)   → AckFloor 유지, pending/redelivery만 초기화

func main() {
	log.Println("=== FilterSubjects 런타임 변경 PoC — NATS 2.14 Consumer Reset API ===")
	log.Println("목적: Reset API로 AckFloor를 리셋해 완전 교체 시 데이터 손실 없이 모든 메시지 수신 가능한지 검증")
	log.Println("비교 기준: 2.12.2 PoC (완전 교체 시 b 5개 중 2개 손실)")

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	stream := initStream(ctx, js)

	log.Println("\n" + hline('=', 60))
	log.Println("  Phase R-1 ★ — 완전 교체 + Reset  [a] → [b]")
	log.Println(hline('=', 60))
	phaseR1(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  Phase R-2 — 추가 + Reset  [b,c] → [a,b,c]")
	log.Println(hline('=', 60))
	phaseR2(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  Phase R-3 — 제거 (Reset 없음, 대조군)  [a,b,c] → [a,b]")
	log.Println(hline('=', 60))
	phaseR3(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  Phase R-4 — Reset 옵션 비교 (nil vs seq=N)")
	log.Println(hline('=', 60))
	phaseR4(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  PoC 완료 — readme-nats-2.14.md 결과 표를 위 로그로 채우세요")
	log.Println(hline('=', 60))
}

// ─── 스트림 초기화 ──────────────────────────────────────────────────────────────

func initStream(ctx context.Context, js jetstream.JetStream) jetstream.Stream {
	_ = js.DeleteStream(ctx, streamName)
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subA, subB, subC},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf(">> Stream '%s' 생성 (Retention=Limits, Storage=Memory)", streamName)
	return stream
}

// ─── 발행 + 첫 seq 반환 ────────────────────────────────────────────────────────

func publishInterleaved(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) uint64 {
	if err := stream.Purge(ctx); err != nil {
		log.Fatalf("Purge 실패: %v", err)
	}
	for i := 1; i <= rounds; i++ {
		for _, sub := range []string{subA, subB, subC} {
			data := fmt.Sprintf("%s-%03d", sub[5:], i)
			if err := nc.Publish(sub, []byte(data)); err != nil {
				log.Fatalf("발행 실패 %s: %v", sub, err)
			}
		}
	}
	_ = nc.Flush()

	info, err := stream.Info(ctx)
	if err != nil {
		log.Fatalf("Stream Info 실패: %v", err)
	}
	firstSeq := info.State.FirstSeq
	log.Printf(">> %d라운드 인터리브 발행 완료 (총 %d개, firstSeq=%d)",
		rounds, rounds*3, firstSeq)
	log.Printf("   seq: a=%d,%d,%d,%d,%d | b=%d,%d,%d,%d,%d | c=%d,%d,%d,%d,%d",
		firstSeq, firstSeq+3, firstSeq+6, firstSeq+9, firstSeq+12,
		firstSeq+1, firstSeq+4, firstSeq+7, firstSeq+10, firstSeq+13,
		firstSeq+2, firstSeq+5, firstSeq+8, firstSeq+11, firstSeq+14)
	return firstSeq
}

// ─── Consumer Reset API ────────────────────────────────────────────────────────

type jsErrResp struct {
	Error *struct {
		Code        int    `json:"code"`
		Description string `json:"description"`
	} `json:"error,omitempty"`
}

// resetConsumer: seq>0 이면 AckFloor=seq-1로 리셋 / seq=0 이면 빈 페이로드(AckFloor 유지)
func resetConsumer(nc *nats.Conn, consumerName string, seq uint64) error {
	subj := fmt.Sprintf("$JS.API.CONSUMER.RESET.%s.%s", streamName, consumerName)
	var payload []byte
	if seq > 0 {
		payload = []byte(fmt.Sprintf(`{"seq":%d}`, seq))
	}
	msg, err := nc.Request(subj, payload, 5*time.Second)
	if err != nil {
		return fmt.Errorf("request 실패: %w", err)
	}
	var resp jsErrResp
	_ = json.Unmarshal(msg.Data, &resp)
	if resp.Error != nil {
		return fmt.Errorf("서버 에러 %d: %s", resp.Error.Code, resp.Error.Description)
	}
	return nil
}

// ─── ConsumerInfo 스냅샷 ───────────────────────────────────────────────────────

type snap struct {
	NumPending     uint64
	NumAckPending  int
	AckFloorStream uint64
}

func snapshot(ctx context.Context, stream jetstream.Stream, name, label string) snap {
	c, err := stream.Consumer(ctx, name)
	if err != nil {
		log.Printf("   [%s|%s] 조회 실패: %v", name, label, err)
		return snap{}
	}
	ci, err := c.Info(ctx)
	if err != nil {
		log.Printf("   [%s|%s] Info 실패: %v", name, label, err)
		return snap{}
	}
	log.Printf("   [%s | %s] NumPending=%d  NumAckPending=%d  AckFloor.Stream=%d  Delivered.Stream=%d",
		name, label, ci.NumPending, ci.NumAckPending, ci.AckFloor.Stream, ci.Delivered.Stream)
	return snap{ci.NumPending, ci.NumAckPending, ci.AckFloor.Stream}
}

// ─── Fetch/Ack 헬퍼 ────────────────────────────────────────────────────────────

func fetchAndAckN(ctx context.Context, cons jetstream.Consumer, n int, label string) {
	for i := 0; i < n; i++ {
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
		if err != nil {
			log.Printf("   [%s] Fetch 오류 (i=%d): %v", label, i, err)
			return
		}
		got := false
		for msg := range batch.Messages() {
			meta, _ := msg.Metadata()
			log.Printf("   [%s] ack: stream_seq=%d  data=%s",
				label, meta.Sequence.Stream, string(msg.Data()))
			_ = msg.Ack()
			got = true
		}
		if !got {
			return
		}
	}
}

func fetchWithTimeout(ctx context.Context, cons jetstream.Consumer, timeout time.Duration) []string {
	var received []string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		wait := remaining
		if wait > 300*time.Millisecond {
			wait = 300 * time.Millisecond
		}
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(wait))
		if err != nil {
			break
		}
		for msg := range batch.Messages() {
			meta, _ := msg.Metadata()
			received = append(received, fmt.Sprintf("seq=%d:%s", meta.Sequence.Stream, string(msg.Data())))
			_ = msg.Ack()
		}
	}
	return received
}

func countBySubject(msgs []string) (a, b, c int) {
	for _, m := range msgs {
		switch {
		case strings.Contains(m, ":a-"):
			a++
		case strings.Contains(m, ":b-"):
			b++
		case strings.Contains(m, ":c-"):
			c++
		}
	}
	return
}

func hline(ch byte, n int) string {
	return strings.Repeat(string(ch), n)
}

// ─── Phase R-1 ★: 완전 교체 [a]→[b] + Reset ───────────────────────────────────

func phaseR1(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("시나리오: filter=[a] → a 3개 ack → filter=[b] → Reset(seq=firstSeq)")
	log.Println("2.12.2 결과: b 5개 중 2개 손실 / 2.14 기대: 5개 전부 수신")
	log.Println(hline('-', 55))

	firstSeq := publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r1-test",
		FilterSubjects: []string{subA},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r1-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: r1-test  FilterSubjects=[test.a]")

	log.Println(">> test.a 3개 ack")
	fetchAndAckN(ctx, cons, 3, "r1")

	before := snapshot(ctx, stream, "r1-test", "filter 변경 전")

	log.Println(">> filter 변경: [test.a] → [test.b]")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r1-test",
		FilterSubjects: []string{subB},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r1-test 업데이트 실패: %v", err)
	}

	afterUpdate := snapshot(ctx, stream, "r1-test", "filter 변경 후 (Reset 전)")
	log.Printf("   AckFloor: %d → %d (리셋 전, b 메시지 일부 AckFloor 이하)",
		before.AckFloorStream, afterUpdate.AckFloorStream)

	// ── Reset API 호출 ──
	log.Printf(">> Consumer Reset: seq=%d (이 phase 첫 seq → AckFloor=%d로 리셋)",
		firstSeq, firstSeq-1)
	log.Printf("   호출: $JS.API.CONSUMER.RESET.%s.r1-test  payload={\"seq\":%d}", streamName, firstSeq)
	if err := resetConsumer(nc, "r1-test", firstSeq); err != nil {
		log.Printf(">> [R-1] Reset 실패: %v", err)
		return
	}
	log.Println(">> [R-1] Reset 성공")

	afterReset := snapshot(ctx, stream, "r1-test", "Reset 후")
	log.Printf("   AckFloor 변화: %d → %d → %d",
		before.AckFloorStream, afterUpdate.AckFloorStream, afterReset.AckFloorStream)

	// Reset 후 consumer handle 재취득 (서버 상태 반영)
	cons, _ = stream.Consumer(ctx, "r1-test")

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [R-1 수신] 총 %d개: a=%d, b=%d, c=%d", len(received), a, b, c)
	for _, m := range received {
		log.Printf("   %s", m)
	}
	log.Printf(">> [R-1 결과] test.b 수신: %d개", b)
	switch b {
	case 5:
		log.Println("   ✓ 5개 전부 수신 — Reset으로 AckFloor 오염 해결. 데이터 손실 없음.")
		log.Println("   2.12.2: 3개(2개 손실) → 2.14+Reset: 5개(손실 없음) ★")
	case 3:
		log.Printf("   ✗ 여전히 3개만 수신 — Reset이 AckFloor에 영향을 주지 않음. 추가 분석 필요.")
	default:
		log.Printf("   ? %d개 수신 — 예상 밖. 수신 목록 직접 확인.", b)
	}
	if a > 0 {
		log.Printf("   ! a 메시지 %d개 재전달됨 — filter=[b]인데 a가 왔다면 예상 밖.", a)
	} else {
		log.Println("   ✓ a 메시지 재전달 없음 — filter=[b]가 올바르게 동작.")
	}
}

// ─── Phase R-2: 추가 [b,c]→[a,b,c] + Reset ────────────────────────────────────

func phaseR2(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("시나리오: filter=[b,c] → b,c 5개 ack → filter=[a,b,c] → Reset(seq=firstSeq)")
	log.Println("핵심 확인: ①a 전부(5개) 수신  ②acked b,c 재전달 여부 (부작용 검증)")
	log.Println("2.12.2 결과: a 2개만(3개 skip) / 2.14 기대: a 5개 수신, 단 b,c 재전달 부작용 가능")
	log.Println(hline('-', 55))

	firstSeq := publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r2-test",
		FilterSubjects: []string{subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r2-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: r2-test  FilterSubjects=[test.b, test.c]")

	log.Println(">> b,c 5개 ack (b:3, c:2)")
	fetchAndAckN(ctx, cons, 5, "r2")
	// 인터리브 순서: b1,c1,b2,c2,b3 ack → AckFloor ≈ firstSeq+7

	before := snapshot(ctx, stream, "r2-test", "filter 변경 전")

	log.Println(">> filter 변경: [test.b, test.c] → [test.a, test.b, test.c]")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r2-test",
		FilterSubjects: []string{subA, subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r2-test 업데이트 실패: %v", err)
	}

	afterUpdate := snapshot(ctx, stream, "r2-test", "filter 변경 후 (Reset 전)")
	log.Printf("   2.12.2와 같은 상태: a 중 AckFloor(%d) 이하 seq는 수신 불가",
		afterUpdate.AckFloorStream)

	// ── Reset API 호출 ──
	log.Printf(">> Consumer Reset: seq=%d → AckFloor=%d로 리셋 (a 전체 포함)", firstSeq, firstSeq-1)
	if err := resetConsumer(nc, "r2-test", firstSeq); err != nil {
		log.Printf(">> [R-2] Reset 실패: %v", err)
		return
	}
	log.Println(">> [R-2] Reset 성공")

	afterReset := snapshot(ctx, stream, "r2-test", "Reset 후")
	log.Printf("   AckFloor 변화: %d → %d → %d",
		before.AckFloorStream, afterUpdate.AckFloorStream, afterReset.AckFloorStream)

	cons, _ = stream.Consumer(ctx, "r2-test")

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [R-2 수신] 총 %d개: a=%d, b=%d, c=%d", len(received), a, b, c)
	for _, m := range received {
		log.Printf("   %s", m)
	}

	log.Printf(">> [R-2 ①] test.a 수신: %d개 (기대 5개)", a)
	if a == 5 {
		log.Println("   ✓ a 전부 수신 — Reset으로 AckFloor 이전 a도 수신 가능.")
	} else {
		log.Printf("   ✗ a %d개만 수신.", a)
	}

	log.Printf(">> [R-2 ②] acked b,c 재전달: b=%d개, c=%d개", b, c)
	if b > 0 || c > 0 {
		log.Printf("   ! acked b,c가 재전달됨 — Reset은 이미 acked 메시지도 재전달시킴.")
		log.Println("   실무 주의: 추가 케이스에서 Reset 사용 시 중복 처리 방지 로직 필요.")
	} else {
		log.Println("   ✓ acked b,c 재전달 없음 — 예상보다 깔끔한 동작.")
	}
}

// ─── Phase R-3: 제거 [a,b,c]→[a,b] (Reset 없음, 대조군) ───────────────────────

func phaseR3(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("시나리오: filter=[a,b,c] → 6개 ack → filter=[a,b] → Reset 없이 수신 확인")
	log.Println("기대: 2.12.2와 동일 — 제거는 원래도 안전, 2.14에서도 동일한지 확인")
	log.Println(hline('-', 55))

	publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r3-test",
		FilterSubjects: []string{subA, subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r3-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: r3-test  FilterSubjects=[test.a, test.b, test.c]")

	log.Println(">> a,b,c 6개 ack (각 2개씩, 남은 c pending: 3개)")
	fetchAndAckN(ctx, cons, 6, "r3")

	before := snapshot(ctx, stream, "r3-test", "filter 변경 전")

	log.Println(">> filter 변경: [test.a, test.b, test.c] → [test.a, test.b]  (Reset 없음)")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r3-test",
		FilterSubjects: []string{subA, subB},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r3-test 업데이트 실패: %v", err)
	}

	after := snapshot(ctx, stream, "r3-test", "filter 변경 후")
	diff := int64(before.NumPending) - int64(after.NumPending)
	log.Printf(">> NumPending 변화: %d → %d (차이=%d, c pending 3개 제거됐으면 ≈3)",
		before.NumPending, after.NumPending, diff)

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [R-3 수신] 총 %d개: a=%d, b=%d, c=%d", len(received), a, b, c)
	log.Printf(">> [R-3 결과] c=%d개 (기대 0) — 제거 즉시 효력, 2.14에서도 동일한지 확인", c)
	if c == 0 && diff == 3 {
		log.Println("   ✓ 2.12.2와 동일 거동. 제거는 2.14에서도 안전.")
	}
}

// ─── Phase R-4: Reset 옵션 비교 (nil vs seq=N) ────────────────────────────────

func phaseR4(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Reset 페이로드 옵션 비교: 빈 페이로드(nil) vs seq=N 지정")
	log.Println("목적: AckFloor 변화 수치 직접 확인")
	log.Println(hline('-', 55))

	firstSeq := publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r4-test",
		FilterSubjects: []string{subA},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("r4-test 생성 실패: %v", err)
	}

	log.Println(">> r4-test 생성, a 3개 ack")
	fetchAndAckN(ctx, cons, 3, "r4")
	base := snapshot(ctx, stream, "r4-test", "ack 후 기준선")

	// ── 케이스 A: 빈 페이로드 reset ──
	log.Println("\n  [케이스 A] 빈 페이로드 Reset (seq 미지정)")
	log.Printf("  ADR-60 기대: AckFloor.Stream 유지(%d), pending·redelivery 초기화", base.AckFloorStream)
	if err := resetConsumer(nc, "r4-test", 0); err != nil {
		log.Printf("  Reset(nil) 실패: %v", err)
	} else {
		afterNil := snapshot(ctx, stream, "r4-test", "nil reset 후")
		if afterNil.AckFloorStream == base.AckFloorStream {
			log.Printf("  ✓ AckFloor 유지: %d → %d", base.AckFloorStream, afterNil.AckFloorStream)
		} else {
			log.Printf("  ? AckFloor 변화: %d → %d (예상 유지)", base.AckFloorStream, afterNil.AckFloorStream)
		}
	}

	// ack 상태 재설정을 위해 컨슈머 재생성
	_ = stream.DeleteConsumer(ctx, "r4-test")
	publishInterleaved(ctx, stream, nc) // 새 seq로 재발행
	firstSeq2Arr := [1]uint64{}
	{
		info, _ := stream.Info(ctx)
		firstSeq2Arr[0] = info.State.FirstSeq
	}
	firstSeq2 := firstSeq2Arr[0]

	cons, _ = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "r4-test",
		FilterSubjects: []string{subA},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	fetchAndAckN(ctx, cons, 3, "r4-2")
	base2 := snapshot(ctx, stream, "r4-test", "ack 후 기준선 (2차)")

	// ── 케이스 B: seq=firstSeq 지정 reset ──
	log.Printf("\n  [케이스 B] seq=%d 지정 Reset → AckFloor=%d 기대", firstSeq2, firstSeq2-1)
	if err := resetConsumer(nc, "r4-test", firstSeq2); err != nil {
		log.Printf("  Reset(seq=%d) 실패: %v", firstSeq2, err)
	} else {
		afterSeq := snapshot(ctx, stream, "r4-test", fmt.Sprintf("seq=%d reset 후", firstSeq2))
		log.Printf("  AckFloor 변화: %d → %d (기대 %d)",
			base2.AckFloorStream, afterSeq.AckFloorStream, firstSeq2-1)
		if afterSeq.AckFloorStream == firstSeq2-1 {
			log.Printf("  ✓ AckFloor = seq-1 확인 (%d → %d)", base2.AckFloorStream, afterSeq.AckFloorStream)
		} else {
			log.Printf("  ? 예상과 다름: %d (기대 %d)", afterSeq.AckFloorStream, firstSeq2-1)
		}
	}

	_ = firstSeq // suppress unused warning
}

package main

import (
	"context"
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

// 발행 순서: a1,b1,c1,a2,b2,c2,...,a5,b5,c5
// stream seq: a=1,4,7,10,13 | b=2,5,8,11,14 | c=3,6,9,12,15
// 인터리브 이유: ack floor(stream seq 기반)와 새 subject 간 충돌을 가시화

func main() {
	log.Println("=== FilterSubjects 런타임 변경 PoC ===")
	log.Println("스트림: FILTER_TEST | subjects: test.{a,b,c}")
	log.Println("발행: 5라운드 인터리브 → seq: a=1,4,7,10,13 | b=2,5,8,11,14 | c=3,6,9,12,15")
	log.Println("업데이트: Go CreateOrUpdateConsumer (= nats consumer edit, 동일 서버 API)")
	log.Println("스코프: LimitsPolicy + MemoryStorage 1조합. Retention/Storage 변경 시 거동 미검증.")

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
	log.Println("  Part 1 — Durable Consumer")
	log.Println(hline('=', 60))
	phaseD1(ctx, stream, nc)
	phaseD2(ctx, stream, nc)
	phaseD3(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  Part 2 — Ephemeral Consumer")
	log.Println(hline('=', 60))
	phaseE0(ctx, stream, nc)
	phaseE1(ctx, stream, nc)

	log.Println("\n" + hline('=', 60))
	log.Println("  PoC 완료 — readme.md 결과 표를 위 로그로 채우세요")
	log.Println(hline('=', 60))
}

// ─── 스트림 초기화 ─────────────────────────────────────────────────────────────

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

// ─── 발행 ──────────────────────────────────────────────────────────────────────

func publishInterleaved(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	if err := stream.Purge(ctx); err != nil {
		log.Fatalf("Purge 실패: %v", err)
	}
	for i := 1; i <= rounds; i++ {
		for _, sub := range []string{subA, subB, subC} {
			data := fmt.Sprintf("%s-%03d", sub[5:], i) // "a-001", "b-002" 형식
			if err := nc.Publish(sub, []byte(data)); err != nil {
				log.Fatalf("발행 실패 %s: %v", sub, err)
			}
		}
	}
	_ = nc.Flush()
	log.Printf(">> %d라운드 인터리브 발행 완료 (총 %d개)", rounds, rounds*3)
}

// ─── ConsumerInfo 스냅샷 ───────────────────────────────────────────────────────

type consSnapshot struct {
	NumPending     uint64
	NumAckPending  int
	AckFloorStream uint64
	DeliveredStream uint64
}

func snapshot(ctx context.Context, stream jetstream.Stream, name, label string) consSnapshot {
	c, err := stream.Consumer(ctx, name)
	if err != nil {
		log.Printf("   [%s|%s] 컨슈머 조회 실패: %v", name, label, err)
		return consSnapshot{}
	}
	ci, err := c.Info(ctx)
	if err != nil {
		log.Printf("   [%s|%s] Info 실패: %v", name, label, err)
		return consSnapshot{}
	}
	log.Printf("   [%s | %s] NumPending=%d  NumAckPending=%d  AckFloor.Stream=%d  Delivered.Stream=%d",
		name, label,
		ci.NumPending, ci.NumAckPending,
		ci.AckFloor.Stream, ci.Delivered.Stream)
	return consSnapshot{
		NumPending:      ci.NumPending,
		NumAckPending:   ci.NumAckPending,
		AckFloorStream:  ci.AckFloor.Stream,
		DeliveredStream: ci.Delivered.Stream,
	}
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
			log.Printf("   [%s] 메시지 없음 (i=%d), 종료", label, i)
			return
		}
	}
}

// fetchWithTimeout은 타임아웃까지 가능한 모든 메시지를 수집하여 ack한다.
// 반환값 형식: "stream_seq=N:data"
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
			entry := fmt.Sprintf("stream_seq=%d:%s", meta.Sequence.Stream, string(msg.Data()))
			received = append(received, entry)
			_ = msg.Ack()
		}
	}
	return received
}

// countBySubject는 "stream_seq=N:x-NNN" 형식의 슬라이스에서 subject별 수를 반환한다.
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

// ─── Phase D-1: 추가 [b,c] → [a,b,c] ─────────────────────────────────────────

func phaseD1(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Phase D-1: 추가  [test.b, test.c] → [test.a, test.b, test.c]")
	log.Println("확인: ①업데이트 성공  ②기존 b,c ack 위치 유지  ③추가된 a 과거 메시지 수신 범위")
	log.Println(hline('-', 55))

	publishInterleaved(ctx, stream, nc)
	// seq: a=1,4,7,10,13 | b=2,5,8,11,14 | c=3,6,9,12,15

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d1-test",
		FilterSubjects: []string{subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("d1-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: d1-test  FilterSubjects=[test.b, test.c]")
	log.Println("   CLI: nats consumer add FILTER_TEST d1-test --filter-subject test.b --filter-subject test.c ...")

	// b,c 인터리브 순서로 5개 ack → seq 2(b1),3(c1),5(b2),6(c2),8(b3) → 3b+2c ack
	// 이후 남은 pending: b=seq11,14(2개), c=seq9,12,15(3개)
	log.Println(">> b,c 합쳐 5개 ack (b:3, c:2 → 나머지 b:2, c:3 pending)")
	fetchAndAckN(ctx, cons, 5, "d1")

	before := snapshot(ctx, stream, "d1-test", "업데이트 전")

	log.Println(">> 업데이트: FilterSubjects=[test.a, test.b, test.c]")
	log.Println("   CLI: nats consumer edit FILTER_TEST d1-test --filter-subject test.a --filter-subject test.b --filter-subject test.c")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d1-test",
		FilterSubjects: []string{subA, subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf(">> [D-1 ①] 업데이트 실패: %v", err)
	} else {
		log.Println(">> [D-1 ①] 업데이트 성공")
	}

	after := snapshot(ctx, stream, "d1-test", "업데이트 후")
	log.Printf("   NumPending 변화: %d → %d", before.NumPending, after.NumPending)

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [D-1 수신] 총 %d개: a=%d개, b=%d개, c=%d개", len(received), a, b, c)
	for _, m := range received {
		log.Printf("   %s", m)
	}
	log.Printf(">> [D-1 ②] b 나머지=%d(기대 2), c 나머지=%d(기대 3) — ack 위치 유지 여부", b, c)
	log.Printf(">> [D-1 ③] 추가된 a 수신=%d개  (AckFloor.Stream=%d 기준 a seq: 1,4,7<floor, 10,13>floor)",
		a, before.AckFloorStream)
}

// ─── Phase D-2: 제거 [a,b,c] → [a,b] ─────────────────────────────────────────

func phaseD2(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Phase D-2: 제거  [test.a, test.b, test.c] → [test.a, test.b]")
	log.Println("확인: ①업데이트 성공  ②제거된 c의 pending 처리(NumPending 변화)  ③a,b ack 위치 무영향")
	log.Println(hline('-', 55))

	publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d2-test",
		FilterSubjects: []string{subA, subB, subC},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("d2-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: d2-test  FilterSubjects=[test.a, test.b, test.c]")

	// 인터리브 순서로 6개 ack → seq 1(a1),2(b1),3(c1),4(a2),5(b2),6(c2) → a:2, b:2, c:2 ack
	// 남은 c pending: seq 9(c3),12(c4),15(c5) → 3개
	log.Println(">> a,b,c 순서로 6개 ack (각 2개씩 → 남은 c pending: 3개)")
	fetchAndAckN(ctx, cons, 6, "d2")

	before := snapshot(ctx, stream, "d2-test", "업데이트 전")

	log.Println(">> 업데이트: FilterSubjects=[test.a, test.b]  (c 제거)")
	log.Println("   CLI: nats consumer edit FILTER_TEST d2-test --filter-subject test.a --filter-subject test.b")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d2-test",
		FilterSubjects: []string{subA, subB},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf(">> [D-2 ①] 업데이트 실패: %v", err)
	} else {
		log.Println(">> [D-2 ①] 업데이트 성공")
	}

	after := snapshot(ctx, stream, "d2-test", "업데이트 후")
	diff := int64(before.NumPending) - int64(after.NumPending)
	log.Printf(">> [D-2 ②] NumPending: %d → %d  (차이=%d, c pending 3개 사라졌으면 ≈3)",
		before.NumPending, after.NumPending, diff)

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [D-2 수신] 총 %d개: a=%d개, b=%d개, c=%d개", len(received), a, b, c)
	for _, m := range received {
		log.Printf("   %s", m)
	}
	if c > 0 {
		log.Printf(">> [D-2 ②] 주목: c 메시지 %d개 수신됨 — filter 제거 후에도 deliver 됨", c)
	} else {
		log.Println(">> [D-2 ②] c 메시지 0개 수신 — filter 제거 즉시 효력")
	}
	log.Printf(">> [D-2 ③] a 나머지=%d(기대 3), b 나머지=%d(기대 3) — ack 위치 무영향 여부", a, b)
}

// ─── Phase D-3: 완전 교체 [a] → [b] ★ ────────────────────────────────────────

func phaseD3(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Phase D-3 ★: 완전 교체  [test.a] → [test.b]  (겹침 0)")
	log.Println("확인: ①교체 성공/거부  ②ack 커서(AckFloor.Stream) 영향  ③b 메시지 몇 개부터 받는가")
	log.Println("인터리브 seq: a=1,4,7,10,13 | b=2,5,8,11,14")
	log.Println(hline('-', 55))

	publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d3-test",
		FilterSubjects: []string{subA},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Fatalf("d3-test 생성 실패: %v", err)
	}
	log.Println(">> 생성: d3-test  FilterSubjects=[test.a]")

	// a 3개 ack → seq 1(a1), 4(a2), 7(a3) → AckFloor.Stream=7
	// b seq: 2(b1),5(b2) < 7 | 8(b3),11(b4),14(b5) > 7
	log.Println(">> test.a 3개 ack → AckFloor.Stream=7 예상")
	log.Println("   b seq 2,5 < AckFloor(7) → 교체 후 skip 여부가 핵심 측정값")
	fetchAndAckN(ctx, cons, 3, "d3")

	before := snapshot(ctx, stream, "d3-test", "업데이트 전")

	log.Println(">> 업데이트: FilterSubjects=[test.b]  (a→b 완전 교체)")
	log.Println("   CLI: nats consumer edit FILTER_TEST d3-test --filter-subject test.b")
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "d3-test",
		FilterSubjects: []string{subB},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf(">> [D-3 ①] 업데이트 실패: %v — 완전 교체 거부됨", err)
		return
	}
	log.Println(">> [D-3 ①] 업데이트 성공")

	after := snapshot(ctx, stream, "d3-test", "업데이트 후")
	log.Printf(">> [D-3 ②] AckFloor.Stream: %d → %d", before.AckFloorStream, after.AckFloorStream)
	log.Printf("   NumPending: %d → %d  (b 5개 기대 vs AckFloor 영향 시 3개 기대)", before.NumPending, after.NumPending)

	received := fetchWithTimeout(ctx, cons, 2*time.Second)
	a, b, c := countBySubject(received)
	log.Printf(">> [D-3 수신] 총 %d개: a=%d개, b=%d개, c=%d개", len(received), a, b, c)
	for _, m := range received {
		log.Printf("   %s", m)
	}
	log.Printf(">> [D-3 ③] test.b 수신: %d개", b)
	switch b {
	case 5:
		log.Println("   결론: AckFloor가 새 subject에 적용되지 않음. b1(seq2), b2(seq5)도 수신됨.")
	case 3:
		log.Printf("   결론: AckFloor(seq %d)가 새 subject에도 적용됨. b1(seq2), b2(seq5) skip → 데이터 손실!", before.AckFloorStream)
	default:
		log.Printf("   결론: 예상 밖 %d개. 위 수신 목록에서 seq 직접 확인 필요.", b)
	}
}

// ─── Phase E-0: Unnamed Ephemeral ─────────────────────────────────────────────

func phaseE0(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Phase E-0: Unnamed Ephemeral (Name/Durable 미설정)")
	log.Println("확인: 서버 할당 이름 확인 → 그 이름으로 FilterSubjects 업데이트 가능한가")
	log.Println(hline('-', 55))

	publishInterleaved(ctx, stream, nc)

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		// Name, Durable 모두 미설정 → 서버가 이름 할당
		FilterSubjects:    []string{subA},
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           30 * time.Second,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: 60 * time.Second,
	})
	if err != nil {
		log.Fatalf("unnamed ephemeral 생성 실패: %v", err)
	}

	ci, _ := cons.Info(ctx)
	serverName := ci.Name
	log.Printf(">> 생성 성공: 서버 할당 이름=%q  Durable=%q  InactiveThreshold=%v",
		serverName, ci.Config.Durable, ci.Config.InactiveThreshold)

	log.Println(">> a 2개 ack")
	fetchAndAckN(ctx, cons, 2, "e0")
	snapshot(ctx, stream, serverName, "업데이트 전")

	log.Printf(">> 서버 할당 이름(%s)을 Name 필드에 넣어 업데이트 시도", serverName)
	_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:              serverName,
		FilterSubjects:    []string{subA, subB},
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           30 * time.Second,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: 60 * time.Second,
	})
	if err != nil {
		log.Printf(">> [E-0] 업데이트 실패: %v", err)
		log.Println("   결론: Unnamed ephemeral은 서버 할당 이름을 알아도 in-place 업데이트 불가")
		log.Println("   실무: filter 변경 = 새 ephemeral 생성 → 이전 ack 상태 유실")
	} else {
		log.Println(">> [E-0] 업데이트 성공 — 서버 할당 이름으로 in-place 업데이트 가능")
		snapshot(ctx, stream, serverName, "업데이트 후")
	}
}

// ─── Phase E-1: Named Ephemeral ───────────────────────────────────────────────

func phaseE1(ctx context.Context, stream jetstream.Stream, nc *nats.Conn) {
	log.Println("\n" + hline('-', 55))
	log.Println("Phase E-1: Named Ephemeral (Name 지정, Durable 미설정)")
	log.Println("확인: 추가 / 제거 / 완전 교체 3케이스에서 in-place 업데이트 가능한가")
	log.Println("      Durable과 동일 동작인가, 아니면 거부되는가")
	log.Println(hline('-', 55))

	cases := []struct {
		label  string
		before []string
		after  []string
	}{
		{"추가 [a]→[a,b]", []string{subA}, []string{subA, subB}},
		{"제거 [a,b,c]→[a,b]", []string{subA, subB, subC}, []string{subA, subB}},
		{"완전 교체 [a]→[b] ★", []string{subA}, []string{subB}},
	}

	ephName := "eph-named-test"

	for _, tc := range cases {
		log.Printf("\n  -- E-1 케이스: %s --", tc.label)

		publishInterleaved(ctx, stream, nc)
		_ = stream.DeleteConsumer(ctx, ephName) // 이전 케이스 잔여 정리

		cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:              ephName,   // 이름 지정
			// Durable 미설정 → ephemeral (InactiveThreshold 후 만료)
			FilterSubjects:    tc.before,
			AckPolicy:         jetstream.AckExplicitPolicy,
			AckWait:           30 * time.Second,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: 60 * time.Second,
		})
		if err != nil {
			log.Printf("  생성 실패: %v", err)
			continue
		}
		log.Printf("  생성: Name=%q  Durable=(없음)  FilterSubjects=%v", ephName, tc.before)

		log.Println("  2개 ack")
		fetchAndAckN(ctx, cons, 2, "e1")
		before := snapshot(ctx, stream, ephName, "업데이트 전")

		log.Printf("  업데이트: FilterSubjects=%v", tc.after)
		log.Printf("  CLI: nats consumer edit FILTER_TEST %s --filter-subject ...", ephName)
		_, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Name:              ephName,
			FilterSubjects:    tc.after,
			AckPolicy:         jetstream.AckExplicitPolicy,
			AckWait:           30 * time.Second,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: 60 * time.Second,
		})
		if err != nil {
			log.Printf("  [E-1] 업데이트 실패: %v", err)
			continue
		}
		log.Println("  [E-1] 업데이트 성공")
		after := snapshot(ctx, stream, ephName, "업데이트 후")
		log.Printf("  NumPending: %d → %d  AckFloor.Stream: %d → %d",
			before.NumPending, after.NumPending,
			before.AckFloorStream, after.AckFloorStream)
	}

	log.Println("\n>> [E-1 종합] Named ephemeral 업데이트 결과:")
	log.Println("   Durable과 동일하게 동작하는지, 아니면 거부/다른 거동인지 위 로그에서 확인")
}

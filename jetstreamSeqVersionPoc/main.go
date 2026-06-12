package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsURL = "nats://localhost:4222"

type result struct {
	Name       string
	Expected   string
	Actual     string
	Pass       bool
	Conclusion string
}

type event struct {
	OrderID string `json:"order_id"`
	Version int64  `json:"version"`
	Note    string `json:"note"`
}

func main() {
	log.SetFlags(0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(500*time.Millisecond))
	must(err, "NATS 연결")
	defer nc.Close()

	js, err := jetstream.New(nc)
	must(err, "JetStream 초기화")

	log.Println("=== JetStream stream seq를 버전 번호로 써도 되는가 PoC ===")
	log.Printf("NATS Server: %s", nc.ConnectedServerVersion())
	log.Printf("nats.go: %s", natsGoVersion())
	log.Println("Subjects: File=order.updated.file, Memory=order.updated.memory, Timestamp=order.updated.ts")

	var results []result
	results = append(results, runStorage(ctx, js, jetstream.FileStorage, "FileStorage")...)
	results = append(results, runStorage(ctx, js, jetstream.MemoryStorage, "MemoryStorage")...)
	results = append(results, runTimestampPhases(ctx, js)...)

	log.Println("\n" + strings.Repeat("=", 78))
	log.Println("결과 요약")
	log.Println(strings.Repeat("=", 78))
	for _, r := range results {
		mark := "✗"
		if r.Pass {
			mark = "✓"
		}
		log.Printf("%s %-34s expected=%-18s actual=%-18s %s", mark, r.Name, r.Expected, r.Actual, r.Conclusion)
	}
}

func runStorage(ctx context.Context, js jetstream.JetStream, storage jetstream.StorageType, label string) []result {
	streamName := fmt.Sprintf("VERSION_SRC_TEST_%s", strings.ToUpper(strings.TrimSuffix(label, "Storage")))
	subject := subjectFor(label)
	log.Println("\n" + strings.Repeat("=", 78))
	log.Printf("Part 1 — JetStream stream seq 거동 (%s, stream=%s)", label, streamName)
	log.Println(strings.Repeat("=", 78))

	var results []result
	results = append(results, phaseS1(ctx, js, streamName, subject, storage, label))
	results = append(results, phaseS2FullPurge(ctx, js, streamName, subject, storage, label))
	results = append(results, phaseS2PurgeKeep(ctx, js, streamName, subject, storage, label))
	results = append(results, phaseS2PurgeSequence(ctx, js, streamName, subject, storage, label))
	results = append(results, phaseS3DeleteRecreate(ctx, js, streamName, subject, storage, label))
	results = append(results, phaseS4SilentDrop(ctx, js, streamName, subject, storage, label))
	return results
}

func phaseS1(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-1 정상 단조 증가", label)
	stream := resetStream(ctx, js, name, subject, storage)
	logStream(ctx, stream, "created")
	seqs := publishMany(ctx, js, subject, 10, "S1")
	log.Printf("   published stream seqs: %v", seqs)
	logStream(ctx, stream, "after publish 10")
	ok := sameSeqs(seqs, 1, 10)
	return result{label + " S-1", "1..10", fmt.Sprint(seqs), ok, "기준선: stream seq는 발행마다 단조 증가"}
}

func phaseS2FullPurge(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-2 전체 Purge 후 seq", label)
	stream := resetStream(ctx, js, name, subject, storage)
	_ = publishMany(ctx, js, subject, 10, "S2-full-before")
	logStream(ctx, stream, "before full purge")
	must(stream.Purge(ctx), "full purge")
	logStream(ctx, stream, "after full purge")
	seq := publishOne(ctx, js, subject, "S2-full-after")
	log.Printf("   first seq after full purge: %d", seq)
	logStream(ctx, stream, "after republish")
	return result{label + " S-2 full purge", "11", fmt.Sprint(seq), seq == 11, "Purge는 last_seq를 초기화하지 않음"}
}

func phaseS2PurgeKeep(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-2 변형 Purge(WithPurgeKeep(2)) 후 seq", label)
	stream := resetStream(ctx, js, name, subject, storage)
	_ = publishMany(ctx, js, subject, 10, "S2-keep-before")
	logStream(ctx, stream, "before purge keep")
	must(stream.Purge(ctx, jetstream.WithPurgeKeep(2)), "purge keep")
	logStream(ctx, stream, "after purge keep=2")
	seq := publishOne(ctx, js, subject, "S2-keep-after")
	log.Printf("   first seq after purge keep: %d", seq)
	logStream(ctx, stream, "after republish")
	return result{label + " S-2 keep=2", "11", fmt.Sprint(seq), seq == 11, "Keep 옵션도 다음 seq는 기존 last_seq+1"}
}

func phaseS2PurgeSequence(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-2 변형 Purge(WithPurgeSequence(6)) 후 seq", label)
	stream := resetStream(ctx, js, name, subject, storage)
	_ = publishMany(ctx, js, subject, 10, "S2-seq-before")
	logStream(ctx, stream, "before purge seq")
	must(stream.Purge(ctx, jetstream.WithPurgeSequence(6)), "purge seq")
	logStream(ctx, stream, "after purge seq=6")
	seq := publishOne(ctx, js, subject, "S2-seq-after")
	log.Printf("   first seq after purge sequence: %d", seq)
	logStream(ctx, stream, "after republish")
	return result{label + " S-2 seq=6", "11", fmt.Sprint(seq), seq == 11, "Sequence 옵션도 last_seq는 유지"}
}

func phaseS3DeleteRecreate(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-3 delete + recreate 후 seq", label)
	stream := resetStream(ctx, js, name, subject, storage)
	_ = publishMany(ctx, js, subject, 10, "S3-before")
	logStream(ctx, stream, "before delete")
	must(js.DeleteStream(ctx, name), "delete stream")
	stream = createStream(ctx, js, name, subject, storage)
	logStream(ctx, stream, "after recreate")
	seq := publishOne(ctx, js, subject, "S3-after")
	log.Printf("   first seq after delete+recreate: %d", seq)
	logStream(ctx, stream, "after republish")
	return result{label + " S-3 recreate", "1", fmt.Sprint(seq), seq == 1, "스트림 재생성은 seq를 1부터 다시 시작"}
}

func phaseS4SilentDrop(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType, label string) result {
	logPhase("S-4 실패 모드: 저장 version 10 > 새 seq", label)
	stream := resetStream(ctx, js, name, subject, storage)
	_ = publishMany(ctx, js, subject, 10, "S4-before")
	storedVersion := uint64(10)
	log.Printf("   DB 저장 last_processed_version(stream seq) = %d", storedVersion)
	must(js.DeleteStream(ctx, name), "delete stream")
	stream = createStream(ctx, js, name, subject, storage)
	logStream(ctx, stream, "after recreate")
	newSeqs := publishMany(ctx, js, subject, 3, "S4-after")
	applied, ignored := 0, 0
	for _, seq := range newSeqs {
		if seq > storedVersion {
			applied++
			storedVersion = seq
			log.Printf("   seq=%d > stored => APPLY", seq)
		} else {
			ignored++
			log.Printf("   seq=%d <= stored(%d) => IGNORE", seq, storedVersion)
		}
	}
	actual := fmt.Sprintf("applied=%d ignored=%d seqs=%v", applied, ignored, newSeqs)
	return result{label + " S-4 silent drop", "applied=0 ignored=3", actual, applied == 0 && ignored == 3, "에러 없이 새 이벤트 전건 무시 재현"}
}

func runTimestampPhases(ctx context.Context, js jetstream.JetStream) []result {
	name := "VERSION_TS_TEST"
	label := "Timestamp"
	log.Println("\n" + strings.Repeat("=", 78))
	log.Printf("Part 2 — 타임스탬프 기반 (%s)", name)
	log.Println(strings.Repeat("=", 78))
	var results []result

	logPhase("T-1 Purge/delete+recreate를 가로질러 타임스탬프 증가", label)
	stream := resetStream(ctx, js, name, "order.updated.ts", jetstream.FileStorage)
	before1 := publishTsEvent(ctx, js, "order.updated.ts", "T1-before-purge", time.Now().UnixNano())
	time.Sleep(time.Millisecond)
	must(stream.Purge(ctx), "timestamp purge")
	logStream(ctx, stream, "after purge")
	afterPurge := publishTsEvent(ctx, js, "order.updated.ts", "T1-after-purge", time.Now().UnixNano())
	time.Sleep(time.Millisecond)
	must(js.DeleteStream(ctx, name), "timestamp delete stream")
	stream = createStream(ctx, js, name, "order.updated.ts", jetstream.FileStorage)
	logStream(ctx, stream, "after recreate")
	afterRecreate := publishTsEvent(ctx, js, "order.updated.ts", "T1-after-recreate", time.Now().UnixNano())
	okT1 := before1 < afterPurge && afterPurge < afterRecreate
	actualT1 := fmt.Sprintf("before=%d afterPurge=%d afterRecreate=%d", before1, afterPurge, afterRecreate)
	log.Printf("   %s", actualT1)
	results = append(results, result{label + " T-1 lifecycle", "before < afterPurge < afterRecreate", actualT1, okT1, "스트림 lifecycle와 무관하게 payload timestamp는 유지"})

	logPhase("T-2-1 같은 millisecond 동률", label)
	stored := time.Now().UnixMilli()
	equal := stored
	applied1 := equal > stored
	log.Printf("   DB stored_ms=%d, incoming_ms=%d, comparison incoming > stored => %v", stored, equal, applied1)
	results = append(results, result{label + " T-2 equal ms", "incoming == stored, > comparison false", fmt.Sprintf("stored=%d incoming=%d apply=%v", stored, equal, applied1), !applied1, "동률에서는 > 비교로 최신 이벤트를 구분할 수 없음"})

	logPhase("T-2-2 클록 스큐/역행", label)
	stored = time.Now().UnixNano()
	lateButSkewedPast := stored - int64(5*time.Second)
	applied2 := lateButSkewedPast > stored
	log.Printf("   실제로 더 늦게 온 이벤트지만 payload timestamp를 5초 과거로 설정")
	log.Printf("   DB stored_ns=%d, incoming_ns=%d, comparison incoming > stored => %v", stored, lateButSkewedPast, applied2)
	results = append(results, result{label + " T-2 skew", "later event can have smaller ts, > comparison false", fmt.Sprintf("stored=%d incoming=%d apply=%v", stored, lateButSkewedPast, applied2), !applied2, "클록 스큐/역행 시 최신 이벤트도 무시될 수 있음"})
	return results
}

func subjectFor(label string) string {
	switch label {
	case "FileStorage":
		return "order.updated.file"
	case "MemoryStorage":
		return "order.updated.memory"
	default:
		return "order.updated.misc"
	}
}

func resetStream(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType) jetstream.Stream {
	_ = js.DeleteStream(ctx, name)
	return createStream(ctx, js, name, subject, storage)
}

func createStream(ctx context.Context, js jetstream.JetStream, name, subject string, storage jetstream.StorageType) jetstream.Stream {
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Storage:   storage,
		Retention: jetstream.LimitsPolicy,
	})
	must(err, "create stream "+name)
	return stream
}

func publishMany(ctx context.Context, js jetstream.JetStream, subject string, n int, tag string) []uint64 {
	seqs := make([]uint64, 0, n)
	for i := 1; i <= n; i++ {
		seqs = append(seqs, publishOne(ctx, js, subject, fmt.Sprintf("%s-%02d", tag, i)))
	}
	return seqs
}

func publishOne(ctx context.Context, js jetstream.JetStream, subject, note string) uint64 {
	body, _ := json.Marshal(event{OrderID: "order-1", Version: 0, Note: note})
	ack, err := js.Publish(ctx, subject, body)
	must(err, "publish "+note)
	return ack.Sequence
}

func publishTsEvent(ctx context.Context, js jetstream.JetStream, subject, note string, ts int64) int64 {
	body, _ := json.Marshal(event{OrderID: "order-1", Version: ts, Note: note})
	ack, err := js.Publish(ctx, subject, body)
	must(err, "publish timestamp "+note)
	log.Printf("   publish %-20s stream_seq=%d payload_ts=%d", note, ack.Sequence, ts)
	return ts
}

func logStream(ctx context.Context, stream jetstream.Stream, label string) {
	info, err := stream.Info(ctx)
	must(err, "stream info "+label)
	log.Printf("   StreamInfo[%s]: msgs=%d first_seq=%d last_seq=%d", label, info.State.Msgs, info.State.FirstSeq, info.State.LastSeq)
}

func logPhase(name, label string) {
	log.Println("\n--- " + label + " " + name + " ---")
}

func sameSeqs(seqs []uint64, start, end uint64) bool {
	if len(seqs) != int(end-start+1) {
		return false
	}
	for i, seq := range seqs {
		if seq != start+uint64(i) {
			return false
		}
	}
	return true
}

func natsGoVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range bi.Deps {
		if dep.Path == "github.com/nats-io/nats.go" {
			return dep.Version
		}
	}
	return "unknown"
}

func must(err error, msg string) {
	if err != nil {
		log.Fatalf("%s 실패: %v", msg, err)
	}
}

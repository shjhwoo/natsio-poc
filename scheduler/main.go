package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsURL = nats.DefaultURL

const StreamName = "scheduledEventSink"
const coreSubject = "schedules.*"

var NC *nats.Conn
var JS jetstream.JetStream

func main() {
	ConnectNats()

	defer NC.Close()

	_, err := JS.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:              StreamName,
		Subjects:          []string{coreSubject, "schedules"},
		Retention:         jetstream.LimitsPolicy,
		AllowMsgSchedules: true,
		AllowMsgTTL:       true,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}

	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 전 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}

	currentTime := time.Now()
	log.Printf("현재 시각: %s", currentTime.Format(time.RFC3339))

	// ----------------------------------------------------------------------
	// 2. [수정] Push Consumer로 변경: JS.Subscribe 사용
	// ----------------------------------------------------------------------

	consumer, err := JS.CreateOrUpdatePushConsumer(context.Background(), StreamName, jetstream.ConsumerConfig{
		DeliverSubject: nats.NewInbox(),
		FilterSubjects: []string{"schedules"},
	})
	if err != nil {
		log.Fatalf("컨슈머 생성 실패: %v", err)
	}

	conCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		log.Println("메세지 수신 완료: ", msg.Subject(), msg.Headers(), string(msg.Data()))
	})
	if err != nil {
		log.Fatalf("메세지 수신 실패: %v", err)
	}

	log.Println("컨슈머 생성완: ", conCtx)

	// ----------------------------------------------------------------------

	scheduledAt := currentTime.Add(15 * time.Second)
	remainingTime := int(scheduledAt.Sub(currentTime).Seconds())

	log.Printf("메세지 발행 예약 시각: %s (예정까지 %d초)", scheduledAt.Format(time.RFC3339), remainingTime)

	pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
		Header: nats.Header{
			"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
			"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime+5)}, // 넉넉하게 TTL 설정
			"Nats-Schedule-Target": []string{"schedules"},                         // Target 주제 설정
		},
		Subject: fmt.Sprintf("schedules.%d", time.Now().UnixNano()), // 원본 발행 주제
		Data:    []byte("This is a scheduled task message"),
	})
	if err != nil {
		log.Fatalf("스케줄된 메세지 발행 실패: %v", err)
	}

	log.Printf("스케줄된 메세지 발행 성공: %+v", pubAck)

	streamInfo, err = JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 후 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}

	select {}
}

func ConnectNats() {
	// ... (연결 함수는 동일)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}

	NC = nc
	log.Println("NATS 서버 연결 성공")

	js, err := jetstream.New(NC)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	JS = js
}

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsURL = "nats://localhost:4228"

const StreamName = "scheduledEventSink"
const coreSubject = "schedules.*"
const republishSubjectForQueueSubscription = "starfruit.internal.pub.event"

var NC *nats.Conn
var JS jetstream.JetStream

func main() {
	ConnectNats()

	defer NC.Close()

	createSchedulerStream()

	currentTime := time.Now()
	log.Printf("현재 시각: %s", currentTime.Format(time.RFC3339))

	queueSubscribeScheduledMessageEvent()

	publishScheduledChatMessageToSchedulerStream(currentTime)

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

func createSchedulerStream() {
	_, err := JS.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:              StreamName,
		Storage:           jetstream.FileStorage,
		Subjects:          []string{coreSubject, "schedules"},
		Retention:         jetstream.LimitsPolicy,
		AllowMsgSchedules: true, // 스케줄된 메세지 허용
		AllowMsgTTL:       true, // 스케줄된 메세지 TTL 허용
		RePublish: &jetstream.RePublish{
			Source:      "schedules",
			Destination: republishSubjectForQueueSubscription,
		},
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}

	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 전 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

// stream에서  예약된 시각에 발행되는 메세지를 구독하는 컨슈머입니다.
// 이 컨슈머를 여러개 생성하면 fanout 방식으로 메세지가 분배됩니다.
func getScheduledMessageFromConsumer() {
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

	log.Println("컨슈머 생성 및 구독 준비 완: ", conCtx)
}

// 스케줄러 stream에서 예약된 시각에 발행되는 메세지를 큐 그룹에게 전달하려면,
// 스케줄러 스트림에 repub 설정을 하고, 신규 subject(repub) 에다가 큐 구독자를 생성해야 합니다.
func queueSubscribeScheduledMessageEvent() {
	for idx := range 2 {
		sub, err := NC.QueueSubscribe(republishSubjectForQueueSubscription, "SCHEDULEQUEUE", func(msg *nats.Msg) {
			log.Printf("%d 번 sub에서, repub 메세지 수신 완료: %s %v %s", idx, msg.Subject, msg.Header, string(msg.Data))
		})
		if err != nil {
			log.Fatalf("컨슈머 생성 실패: %v", err)
		}
		log.Println("repub 이벤트수신을 위한 sub 생성완: ", sub)
	}
}

func publishScheduledChatMessageToSchedulerStream(currentTime time.Time) {
	for idx := range 10 {
		scheduledAt := currentTime.Add(10 * time.Second)
		remainingTime := int(scheduledAt.Sub(currentTime).Seconds())

		pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
			Header: nats.Header{
				"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
				"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime)}, // TTL 설정
				"Nats-Schedule-Target": []string{"schedules"},                       // Target 주제 설정
			},
			Subject: fmt.Sprintf("schedules.%d", time.Now().UnixNano()), // 원본 발행 주제
			Data:    []byte(fmt.Sprintf("This is a scheduled task message %d", idx)),
		})
		if err != nil {
			log.Fatalf("스케줄된 메세지 발행 실패: %v", err)
		}

		log.Printf("스케줄된 메세지 발행 성공: %+v 메세지 발행 예약 시각: %s (예정까지 %d초)", pubAck, scheduledAt.Format(time.RFC3339), remainingTime)

		time.Sleep(1 * time.Second)
	}

	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 후 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsURL = "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"

const StreamName = "scheduledEventSink"
const coreSubjectPrefix = "schedules.pending"
const onProcessSubject = "schedules.onProcess"
const republishSubjectForQueueSubscription = "internalEvent"

var NC *nats.Conn
var JS jetstream.JetStream

func main() {
	ConnectNats()

	defer NC.Close()

	createSchedulerStream()

	getScheduledMessageFromConsumer()

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
		Subjects:          []string{fmt.Sprintf("%s.*", coreSubjectPrefix), onProcessSubject},
		Retention:         jetstream.LimitsPolicy, //- 수정 및 삭제 시 필수 옵션
		Discard:           jetstream.DiscardOld,   //- 수정 및 삭제 시 필수 옵션
		MaxMsgsPerSubject: 1,                      //- 수정 및 삭제 시 필수 옵션
		AllowMsgSchedules: true,                   // 스케줄된 메세지 허용
		AllowMsgTTL:       true,                   // 스케줄된 메세지 TTL 허용
		RePublish: &jetstream.RePublish{
			Source:      onProcessSubject,
			Destination: republishSubjectForQueueSubscription,
		},
		Replicas: 3,
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
		FilterSubjects: []string{onProcessSubject},
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

			PrintStreamInfo()

		})
		if err != nil {
			log.Fatalf("컨슈머 생성 실패: %v", err)
		}
		log.Println("repub 이벤트수신을 위한 sub 생성완: ", sub)
	}
}

func publishScheduledChatMessageToSchedulerStream(currentTime time.Time) {
	for idx := range 3 {

		var scheduleId = fmt.Sprintf("ULID_%d", idx+1)
		var modifyCount = 3

		for i := 1; i <= modifyCount; i++ {

			time.Sleep(1 * time.Second)

			//하나의 메세지 발행해놓고 5초씩 scheduledAt을 앞당겨보자. 항상 최종버전의 메세지만 남아있어야 한다.
			scheduledAt := currentTime.Add(time.Duration(30-5*i) * time.Second)
			pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
				Header: nats.Header{
					"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
					"Nats-Schedule-TTL":    []string{"5s"},             // TTL 설정 (메세지 발행 후 보관을 하고 있을 기간을 의미**)
					"Nats-Schedule-Target": []string{onProcessSubject}, // Target 주제 설정
				},
				Subject: fmt.Sprintf("%s.%s", coreSubjectPrefix, scheduleId),
				Data:    []byte(fmt.Sprintf("This is a scheduled task message with Id %s", scheduleId)),
			})
			if err != nil {
				log.Fatalf("스케줄된 메세지 발행 실패: %v", err)
			}

			if i == modifyCount {
				log.Printf("최종 버전의 스케줄된 메세지(%s) 발행 성공: %+v 메세지 발행 예약 시각: %s (예정까지 %d초)", scheduleId, pubAck, scheduledAt.Format(time.RFC3339), int(scheduledAt.Sub(currentTime).Seconds()))
			}
		}
	}

	PrintStreamInfo()
}

func PrintStreamInfo() {
	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

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
const discardedSubject = "schedules.discard"
const republishSubjectForQueueSubscription = "internalEvent"

var NC *nats.Conn
var JS jetstream.JetStream

func main() {
	ConnectNats()

	defer NC.Close()

	//getScheduledMessageFromConsumer()

	createSchedulerStream()

	currentTime := time.Now()
	log.Printf("현재 시각: %s", currentTime.Format(time.RFC3339))

	queueSubscribeScheduledMessageEvent()

	createPubAck, err := createScheduledChatMessageToSchedulerStream()
	if err != nil {
		log.Fatalf("스케줄된 메세지 발행 실패: %v", err)
	}
	log.Printf("스케줄된 메세지 발행 성공: %+v", createPubAck)
	PrintStreamInfo() //즉시확인

	time.Sleep(time.Second * 6)
	PrintStreamInfo() //스케줄된 메세지 발행 후 6초뒤에 확인

	discardPubAck, err := discardScheduledChatMessage()
	if err != nil {
		log.Fatalf("스케줄된 메세지 삭제요청 실패: %v", err)
	}
	log.Printf("스케줄된 메세지 삭제요청 성공: %+v, 남은시간: 5초 뒤에 삭제됨", discardPubAck)
	time.Sleep(time.Second * 6)
	PrintStreamInfo()

	immediatePubAck, err := immediateSendScheduledChatMessageAtSchedulerStream()
	if err != nil {
		log.Fatalf("스케줄된 메세지 즉시발송 실패: %v", err)
	}
	log.Printf("스케줄된 메세지 즉시발송 성공: %+v", immediatePubAck)
	PrintStreamInfo()

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
		Subjects:          []string{fmt.Sprintf("%s.*", coreSubjectPrefix), onProcessSubject, discardedSubject},
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

// func getScheduledMessageFromConsumer() {
// 	consumer, err := JS.CreateOrUpdatePushConsumer(context.Background(), StreamName, jetstream.ConsumerConfig{
// 		DeliverSubject: nats.NewInbox(),
// 		FilterSubjects: []string{onProcessSubject},
// 	})
// 	if err != nil {
// 		log.Fatalf("컨슈머 생성 실패: %v", err)
// 	}

// 	conCtx, err := consumer.Consume(func(msg jetstream.Msg) {
// 		log.Println("메세지 수신 완료: ", msg.Subject(), msg.Headers(), string(msg.Data()))
// 	})
// 	if err != nil {
// 		log.Fatalf("메세지 수신 실패: %v", err)
// 	}

// 	log.Println("컨슈머 생성 및 구독 준비 완: ", conCtx)
// }

// 스케줄러 stream에서 예약된 시각에 발행되는 메세지를 큐 그룹에게 전달하려면,
// 스케줄러 스트림에 repub 설정을 하고, 신규 subject(repub) 에다가 큐 구독자를 생성해야 합니다.
func queueSubscribeScheduledMessageEvent() {
	sub, err := NC.QueueSubscribe(republishSubjectForQueueSubscription, "SCHEDULEQUEUE", func(msg *nats.Msg) {
		log.Printf(" repub 메세지 수신 완료: %s %v %s", msg.Subject, msg.Header, string(msg.Data))

		log.Println("6초뒤에 스트림 확인")
		time.Sleep(6 * time.Second)
		PrintStreamInfo()

	})
	if err != nil {
		log.Fatalf("컨슈머 생성 실패: %v", err)
	}
	log.Println("repub 이벤트수신을 위한 sub 생성완: ", sub)
}

func createScheduledChatMessageToSchedulerStream() (*jetstream.PubAck, error) {
	var scheduleId = "ULID_1"

	currentTime := time.Now()

	scheduledAt := currentTime.Add(10 * time.Second)
	remainingTime := 5

	pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
		Header: nats.Header{
			"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
			"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime)}, // TTL 설정 (즉 실제 target에 도달 후 보관을 하고 있을 기간을 의미**)
			"Nats-Schedule-Target": []string{onProcessSubject},                  // Target 주제 설정
		},
		Subject: fmt.Sprintf("%s.%s", coreSubjectPrefix, scheduleId),
		Data:    []byte(fmt.Sprintf("<신규>This is a scheduled task message with Id %s", scheduleId)),
	})
	if err != nil {
		return nil, err
	}

	return pubAck, nil
}

func immediateSendScheduledChatMessageAtSchedulerStream() (*jetstream.PubAck, error) {
	var scheduleId = "ULID_2"

	currentTime := time.Now()

	scheduledAt := currentTime
	remainingTime := 5

	pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
		Header: nats.Header{
			"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
			"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime)}, // TTL 설정 (즉 실제 target에 도달 후 보관을 하고 있을 기간을 의미**)
			"Nats-Schedule-Target": []string{onProcessSubject},                  // Target 주제 설정
		},
		Subject: fmt.Sprintf("%s.%s", coreSubjectPrefix, scheduleId),
		Data:    []byte(fmt.Sprintf("<즉시발송>This is a scheduled task message with Id %s", scheduleId)),
	})
	if err != nil {
		return nil, err
	}

	return pubAck, nil
}

func discardScheduledChatMessage() (*jetstream.PubAck, error) {
	var scheduleId = "ULID_1"

	currentTime := time.Now()
	scheduledAt := currentTime.Add(2 * time.Second)
	remainingTime := 5

	pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
		Header: nats.Header{
			"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))}, // 원래 예약 시각 (삭제시에도 필수)
			"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime)},                       // TTL 설정
			"Nats-Schedule-Target": []string{discardedSubject},                                        // Target 주제 설정 (삭제시에도 필수)
		},
		Subject: fmt.Sprintf("%s.%s", coreSubjectPrefix, scheduleId),
		Data:    []byte(fmt.Sprintf("<삭제됨>This is a scheduled task message with Id %s", scheduleId)),
	})
	if err != nil {
		return nil, err
	}

	return pubAck, nil
}

func PrintStreamInfo() {
	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

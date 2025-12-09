package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var NumScheduledChatMessages = 1000
var ModifyCountPerMessage = 10
var InitDelayTime int

var latencyChan = make(chan time.Duration)

const natsURL = "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
const StreamName = "scheduledEventSink"
const coreSubjectPrefix = "schedules.pending"
const onProcessSubject = "schedules.onProcess"
const republishSubjectForQueueSubscription = "internalEvent"

var NC *nats.Conn
var JS jetstream.JetStream

func main() {

	setLoadTestArguments()

	ConnectNats()

	defer NC.Close()

	createSchedulerStream()

	go latencyAggregator()

	queueSubscribeScheduledMessageEvent()

	publishScheduledChatMessageToSchedulerStream()

	err := JS.DeleteStream(context.Background(), StreamName)
	if err != nil {
		log.Printf("기존 스트림 삭제 실패: %v", err)
	}

	select {}
}

func setLoadTestArguments() {
	args := os.Args

	log.Println("args: ", args)
	//         0      1    2
	// go run main.go 1000 10
	if len(args) >= 3 {
		numScheduledChatMessagesInt, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatalf("첫번째 인자(예약메세지수) 변환 실패: %v", err)
		}
		NumScheduledChatMessages = numScheduledChatMessagesInt

		modifyCountPerMessageInt, err := strconv.Atoi(args[2])
		if err != nil {
			log.Fatalf("두번째 인자(메세지수정횟수) 변환 실패: %v", err)
		}
		ModifyCountPerMessage = modifyCountPerMessageInt

		InitDelayTime = ModifyCountPerMessage * (ModifyCountPerMessage + 1) //초
	}

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
	err := JS.DeleteStream(context.Background(), StreamName)
	if err != nil {
		log.Printf("기존 스트림 삭제 실패: %v", err)
	}

	if _, err := JS.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
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
	}); err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}

	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 전 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

// 스케줄러 stream에서 예약된 시각에 발행되는 메세지를 큐 그룹에게 전달하려면,
// 스케줄러 스트림에 repub 설정을 하고, 신규 subject(repub) 에다가 큐 구독자를 생성해야 합니다.
func latencyAggregator() {
	var totalLatency time.Duration
	var maxLatency time.Duration
	var messageCount int64

	// 채널이 닫힐 때까지 반복하며 값을 수신합니다.
	for latency := range latencyChan {

		if latency > maxLatency {
			maxLatency = latency
			log.Printf("🚨 새로운 최대 Latency 기록: %s", maxLatency)
		}

		totalLatency += latency
		messageCount++

		// 주기적으로 평균 Latency 출력 (단일 고루틴이므로 Lock 불필요)
		if messageCount%100 == 0 {
			avgLatency := totalLatency / time.Duration(messageCount)
			log.Printf("📢 Latency Aggregator: 메시지 총 %d개, 현재 평균 Latency: %s, 최대 Latency: %s", messageCount, avgLatency, maxLatency)
		}
	}
}

func queueSubscribeScheduledMessageEvent() {

	sub, err := NC.QueueSubscribe(republishSubjectForQueueSubscription, "SCHEDULEQUEUE", func(msg *nats.Msg) {
		log.Printf("repub 메세지 수신 완료: %s %v %s", msg.Subject, msg.Header, string(msg.Data))
		//TODO Latency 측정 로직 추가

		var content MessageContent
		err := json.Unmarshal(msg.Data, &content)
		if err != nil {
			log.Fatalf("메세지 역직렬화 실패: %v", err)
		}

		// 1. 헤더에서 Nats-Time-Stamp 값 추출
		timestampStr := msg.Header.Get("Nats-Time-Stamp")

		// 2. RFC3339Nano 형식으로 파싱 (NATS의 기본 포맷)
		natsTime, err := time.Parse(time.RFC3339Nano, timestampStr)
		if err != nil {
			log.Fatalf("Nats-Time-Stamp 파싱 실패: %v, 값: %s", err, timestampStr)
		}

		// 3. NATS-Time-Stamp를 기준으로 Latency 재계산
		latency := natsTime.Sub(content.ScheduledAt.In(time.UTC))

		log.Printf("지연 시간 계산 완료: NATS Time: %v, Latency: %v", natsTime, latency)

		// 2. Latency 값을 채널로 전송 (Aggregator 고루틴이 처리)
		// 🚨 주의: Aggregator 고루틴이 시작되어 수신할 준비가 되어 있어야 합니다.
		latencyChan <- latency

	})
	if err != nil {
		log.Fatalf("컨슈머 생성 실패: %v", err)
	}
	log.Println("repub 이벤트수신을 위한 sub 생성완: ", sub)

}

type MessageContent struct {
	ScheduledAt time.Time
	ScheduleId  string
}

func publishScheduledChatMessageToSchedulerStream() {
	for idx := range NumScheduledChatMessages {

		var scheduleId = fmt.Sprintf("ULID_%d", idx+1)

		for mc := 1; mc <= ModifyCountPerMessage; mc++ {

			currentTime := time.Now()

			scheduledAt := currentTime.Add(time.Duration(InitDelayTime-ModifyCountPerMessage*mc) * time.Second)
			remainingTime := int(scheduledAt.Sub(currentTime).Seconds())

			msg := MessageContent{
				ScheduleId:  scheduleId,
				ScheduledAt: scheduledAt,
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				log.Fatalf("메세지 직렬화 실패: %v", err)
			}

			pubAck, err := JS.PublishMsg(context.Background(), &nats.Msg{
				Header: nats.Header{
					"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
					"Nats-Schedule-TTL":    []string{fmt.Sprintf("%ds", remainingTime)}, // TTL 설정
					"Nats-Schedule-Target": []string{onProcessSubject},                  // Target 주제 설정
				},
				Subject: fmt.Sprintf("%s.%s", coreSubjectPrefix, scheduleId),
				Data:    msgBytes,
			})
			if err != nil {
				log.Fatalf("스케줄된 메세지 발행 실패: %v, remainingTime: %d", err, remainingTime)
			}

			if mc == ModifyCountPerMessage {
				log.Printf("최종 버전의 스케줄된 메세지(%s) 발행 성공: %+v 메세지 발행 예약 시각: %s (예정까지 %d초)", scheduleId, pubAck, scheduledAt.Format(time.RFC3339), remainingTime)

				if idx == NumScheduledChatMessages {
					time.Sleep(time.Duration(remainingTime))
					close(latencyChan)
					log.Println("Latency 채널이 닫혔습니다. 집계가 완료되었습니다.")
				}
			}
		}
	}

	//스트림 정책에 따라서 메세지는 최종 3개로만 남아있어야 한다
	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 발행 후 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

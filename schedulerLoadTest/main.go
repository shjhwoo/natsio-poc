package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var NumScheduledChatMessages = 1000
var ModifyCountPerMessage = 10
var InitDelayTime int
var ScheduleTimeIntervalMin = 1
var ScheduledMsgGroupingCount = 100

var latencyChan = make(chan time.Duration, 100)

const natsURL = "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
const StreamName = "scheduledEventSink"
const coreSubjectPrefix = "schedules.pending"
const onProcessSubject = "schedules.onProcess"
const republishSubjectForQueueSubscription = "internalEvent"

var NC *nats.Conn
var JS jetstream.JetStream

var KST *time.Location

var wg sync.WaitGroup // <--- 추가

var FirstScheduledAt time.Time
var LastScheduledAt time.Time
var totalLatency time.Duration
var maxLatency time.Duration
var messageCount int64

func main() {

	kst, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		log.Fatalf("시간대 로드 실패: %v", err)
	}
	KST = kst

	setLoadTestArguments()

	ConnectNats()

	defer NC.Close()

	createSchedulerStream()

	go latencyAggregator()

	queueSubscribeScheduledMessageEvent()

	// 메시지 수만큼 WaitGroup 카운터 설정
	wg.Add(NumScheduledChatMessages) // <--- 추가

	publishScheduledChatMessageToSchedulerStream()

	log.Println("--- 모든 메시지 발행 완료 및 수신 대기 시작 ---")

	// 모든 메시지 처리가 완료될 때까지 대기
	wg.Wait()

	log.Println("--- 모든 메시지 수신 및 집계 완료. 채널 닫기 ---")
	close(latencyChan)

	log.Println("모든 예약메세지 발행 및 수신 테스트 완료, FirstScheduledAt:", FirstScheduledAt, "LastScheduledAt:", LastScheduledAt)
	schDuration := LastScheduledAt.Sub(FirstScheduledAt)

	if schDuration.Minutes() > 0 {
		log.Printf("분당 %d건의 예약메세지 발행 및 수신 테스트 완료:", NumScheduledChatMessages/int(schDuration.Minutes()))
	}

	avgLatency := totalLatency / time.Duration(messageCount)
	log.Println("📢 Latency Aggregator - avgLatency: ", avgLatency, "maxLatency", maxLatency, "count: ", messageCount)
}

func setLoadTestArguments() {
	args := os.Args

	log.Println("args: ", args)
	//         0      1    2
	// go run main.go 1000 10
	if len(args) >= 5 {
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

		InitDelayTime = ModifyCountPerMessage

		scheduleTimeIntervalMinInt, err := strconv.Atoi(args[3])
		if err != nil {
			log.Fatalf("세번째 인자(메세지그룹간 ScheduledAt 간격(분)) 변환 실패: %v", err)
		}
		ScheduleTimeIntervalMin = scheduleTimeIntervalMinInt

		scheduledMsgGroupingCountInt, err := strconv.Atoi(args[4])
		if err != nil {
			log.Fatalf("네번째 인자(메세지그룹핑수) 변환 실패: %v", err)
		}
		ScheduledMsgGroupingCount = scheduledMsgGroupingCountInt
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

func latencyAggregator() {

	// 채널이 닫힐 때까지 반복하며 값을 수신합니다.
	for latency := range latencyChan {

		if latency.Seconds() >= 0 {
			log.Println("latency", latency)

			if latency > maxLatency {
				maxLatency = latency
				log.Printf("🚨 새로운 최대 Latency 기록: %s", maxLatency)
			}

			totalLatency += latency
		}

		messageCount++
	}

}

func queueSubscribeScheduledMessageEvent() {

	sub, err := NC.QueueSubscribe(republishSubjectForQueueSubscription, "SCHEDULEQUEUE", func(msg *nats.Msg) {
		log.Printf("repub 메세지 수신 완료: %s %v %s", msg.Subject, msg.Header, string(msg.Data))

		var content MessageContent
		err := json.Unmarshal(msg.Data, &content)
		if err != nil {
			log.Fatalf("메세지 역직렬화 실패: %v", err)
		}

		// 3. NATS-Time-Stamp를 기준으로 Latency 재계산
		latency := time.Now().In(KST).Sub(content.ScheduledAt)

		// 2. Latency 값을 채널로 전송 (Aggregator 고루틴이 처리)
		// 🚨 주의: Aggregator 고루틴이 시작되어 수신할 준비가 되어 있어야 합니다.
		latencyChan <- latency
		wg.Done()
		log.Printf("지연 시간 계산 완료: now: %v, scheduledAt : %v,  Latency: %v", time.Now().In(KST), content.ScheduledAt, latency)

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

// 100개 5분간격 테스트
func publishScheduledChatMessageToSchedulerStream() {
	for idx := range NumScheduledChatMessages {

		var scheduleId = fmt.Sprintf("ULID_%d", idx+1)

		fastestScheduledAt := getFastestScheduledAtbyIdx(idx)

		for mc := 1; mc <= ModifyCountPerMessage; mc++ {

			scheduledAt := fastestScheduledAt.Add(time.Duration(InitDelayTime-mc) * time.Minute)

			if idx == 0 && mc == ModifyCountPerMessage {
				FirstScheduledAt = scheduledAt
			}
			if idx == NumScheduledChatMessages-1 && mc == ModifyCountPerMessage {
				LastScheduledAt = scheduledAt
			}

			remainingTime := int(scheduledAt.Sub(time.Now().In(KST)).Seconds())

			msg := MessageContent{
				ScheduleId:  scheduleId,
				ScheduledAt: scheduledAt,
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				log.Printf("메세지 직렬화 실패: %v", err)
				continue
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
			}
		}
	}

	PrintStreamInfo()
}

func getFastestScheduledAtbyIdx(idx int) time.Time {

	fatestNextMinuteTime := getFatestNextMinuteTime()

	//scheduledMsgGroupingCount(개) 마다, scheduleTimeIntervalMin(분) 씩 증가

	d := idx / ScheduledMsgGroupingCount

	add := ScheduleTimeIntervalMin * (d + 1)

	return fatestNextMinuteTime.Add(time.Duration(add) * time.Minute)
}

func getFatestNextMinuteTime() time.Time {
	currentTime := time.Now().In(KST)
	currentSec := currentTime.Second()
	currentTimeWithoutSec := currentTime.Add(time.Duration(-currentSec) * time.Second)
	return currentTimeWithoutSec.Add(1 * time.Minute)
}

func PrintStreamInfo() {
	streamInfo, err := JS.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 Stream 상태: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}

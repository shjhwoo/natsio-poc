package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATS 서버 주소 및 이벤트 주제 정의
const natsURL = nats.DefaultURL
const coreSubject = "starfruit.eventSink.roomCreated"

// 각 서비스별 큐 그룹 정의
var serviceQueues = map[string]string{
	"채팅 서비스": "chatServiceQueue",
	"파일 서비스": "fileServiceQueue",
	"로깅 서비스": "logServiceQueue",
}

// 총 기대 작업량(메시지 수 * 큐 그룹 수)을 추적하기 위한 WaitGroup
var wg sync.WaitGroup

func main() {
	log.Println("--- NATS 다중 Queue Group POC 시작 ---")

	// 1. NATS 연결
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()
	log.Println("NATS 서버 연결 성공")

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 초기화 실패: %v", err)
	}

	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "eventSink",
		Subjects: []string{"starfruit.eventSink.>"},
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}

	// 2. 구독자 셋업 (각 서비스당 2개의 인스턴스)
	numInstancesPerService := 2
	//totalSubscribers := len(serviceQueues) * numInstancesPerService

	log.Printf("총 %d개 서비스 (%d개 큐 그룹), 각 서비스당 %d개 인스턴스 실행 중...", len(serviceQueues), len(serviceQueues), numInstancesPerService)

	for serviceName, queueName := range serviceQueues {
		for i := 1; i <= numInstancesPerService; i++ {
			go runSubscriber(stream, serviceName, i, nc, queueName)
		}
	}

	// 3. 잠시 대기 (구독자들이 NATS에 연결될 시간)
	time.Sleep(1 * time.Second)

	// 4. 메시지 발행 (총 15개)
	numMessages := 15

	// 총 작업량 설정: 메시지 15개 * 3개 큐 그룹 = 45개 작업
	wg.Add(numMessages * len(serviceQueues))

	log.Printf("\n>>> 메시지 발행 시작: %d개 이벤트를 '%s' 주제로 전송", numMessages, coreSubject)
	for i := 1; i <= numMessages; i++ {
		eventData := fmt.Sprintf("Room_ID: R%03d", i)
		nc.Publish(coreSubject, []byte(eventData))
		time.Sleep(50 * time.Millisecond) // 전송 간격
	}
	log.Println("<<< 메시지 발행 완료")

	// 5. 모든 구독자가 작업을 처리할 때까지 대기
	wg.Wait()
	time.Sleep(1 * time.Second) // 최종 출력 확인을 위한 대기
	log.Println("\n--- POC 종료 (총 45개 작업 완료) ---")
}

// runSubscriber는 각 서비스의 개별 워커(인스턴스)를 실행합니다.
func runSubscriber(stream jetstream.Stream, serviceName string, instanceID int, nc *nats.Conn, queueName string) {

	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:   queueName,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("컨슈머 생성 실패: %v", err)
	}

	workerID := fmt.Sprintf("%s-%d", serviceName, instanceID)

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		defer msg.Ack()

		// 처리 시간 시뮬레이션: 워커 ID에 따라 처리 시간을 다르게 설정하여 부하 분산 확인
		processTime := time.Duration((instanceID*2+len(serviceName))%100+50) * time.Millisecond
		time.Sleep(processTime)

		// 큐 그룹 이름을 출력하여 해당 메시지가 어떤 그룹에 의해 처리되었는지 확인합니다.
		log.Printf("[처리 완료] Worker: %s | Queue Group: %s | Data: %s | Time: %v",
			workerID, queueName, string(msg.Data()), processTime)

	})
	if err != nil {
		log.Fatalf("메시지 소비 실패: %v", err)
	}

	log.Println("구독자:", workerID, "큐 그룹:", queueName, "consumeContext:", consumeCtx)
}

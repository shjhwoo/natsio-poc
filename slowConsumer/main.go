package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const NatsURL = nats.DefaultURL
const Subject = "orders.create"

var natsConn *nats.Conn

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ConnectNats()

	StartQueueSubscribers()

	StartEventPublisher()
}

func ConnectNats() {
	// 1. NATS 연결 설정
	nc, err := nats.Connect(
		NatsURL,
		// 📢 v1.47.0에서 디버그 로그가 없으므로, 핸들러로 상태 변화를 포착합니다.
		nats.DisconnectHandler(func(nc *nats.Conn) {
			log.Printf("🚨 연결 끊김 감지! 최종 에러: %v", nc.LastError())
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Println("✅ 연결 복구 성공!")
		}),
		nats.MaxReconnects(5),
		nats.ErrorHandler(natsErrHandler),
	)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}

	natsConn = nc
	log.Printf("NATS에 연결되었습니다: %s", nc.ConnectedUrl())
}

func natsErrHandler(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
	fmt.Printf("natsErrHandler error: %v\n", natsErr)
	if natsErr == nats.ErrSlowConsumer {
		pendingMsgs, _, err := sub.Pending()
		if err != nil {
			fmt.Printf("couldn't get pending messages: %v", err)
			return
		}
		fmt.Printf("Falling behind with %d pending messages on subject %q.\n", pendingMsgs, sub.Subject)
		// Log error, notify operations...

	}
	// check for other errors
}

func StartQueueSubscribers() {
	// 2. 구독 설정 (QueueSubscribe를 사용하여 여러 워커 가정)
	// 구독이 NATS 클라이언트의 내부 버퍼를 사용할 때 문제가 발생합니다.
	sub, err := natsConn.QueueSubscribe(Subject, "slow-group", func(msg *nats.Msg) {
		// 📢 이 핸들러는 메시지 하나당 500ms를 소비하며 '느린 작업'을 시뮬레이션합니다.
		log.Printf("메시지 수신 [%s] - 처리 시작", msg.Header.Get("index"))
		time.Sleep(5000 * time.Millisecond) // 🐌 느린 작업 시뮬레이션
		log.Printf("메시지 수신 [%s] - 처리 완료", msg.Header.Get("index"))
	})
	if err != nil {
		log.Fatalf("구독 실패: %v", err)
	}

	log.Println("구독 시작. 이제 publisher를 실행하여 메시지를 보내세요.", sub.Queue)

	// 3. 📢  Pending Limits를 설정하여 버퍼를 확장 (이 코드가 없으면 문제가 재현됨)
	// 이 코드를 주석 처리(기본 버퍼 사용)하거나, MaxMsgs/MaxBytes를 작게 설정하여 재현할 수 있습니다.
	err = sub.SetPendingLimits(10, 1024*1024) // 기본값과 동일하게 설정하여 효과를 없앰
	//err = sub.SetPendingLimits(20000, 50*1024*1024) // 💡 이 줄을 활성화하면 Slow Consumer가 해결됨
	if err != nil {
		log.Fatalf("Pending Limits 설정 실패: %v", err)
	}
}

func StartEventPublisher() {
	var i int = 1
	for {

		msg := nats.NewMsg(Subject)

		msg.Data = fmt.Appendf(nil, "Order #%d - Payload Data", i)

		// 메시지 헤더에 인덱스 추가 (로그 확인용)
		msg.Header.Set("index", fmt.Sprintf("%d", i))

		// 메시지를 발행합니다.
		if err := natsConn.PublishMsg(msg); err != nil {
			log.Printf("메시지 발행 실패 #%d: %v", i, err)
			time.Sleep(10 * time.Millisecond) // 잠시 대기 후 재시도 시뮬레이션
			continue
		}

		// 📢 발행 간격을 짧게 설정하여 구독자의 처리 속도(5000ms)보다 빠르게 만듭니다.
		time.Sleep(50 * time.Millisecond)
		i++
	}
}

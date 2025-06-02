package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var NC *nats.Conn
var TaskStream jetstream.Stream

func main() {

	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo("starfruit", "mgrsol123"))
	if err != nil {
		log.Println("Error connecting to NATS server:", err)
		return
	}
	NC = nc

	js, err := jetstream.New(nc)
	if err != nil {
		log.Println("Error creating js api:", err)
		return
	}

	stream, err := SetupTaskStream(js)
	if err != nil {
		log.Println("Error setting up stream:", err)
		return
	}
	TaskStream = stream

	//태스크 메세지를 DB에서 가지고 와서 정렬한 다음에 TASK 스트림에 넣어주고,

	go GetTaskListFromDBAndInsertToTaskStream()

	go ListenScheduledMsg()

	//컨슈머가 쭉 빼오면서
	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "H00017_SCHEDULED_MSG_TASK_CONSUMER",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 5, //기본 최대 1000개까지 펜딩가능. 그 이상은 서버에서 미룬다고함. 제한을 두지 않으려면 -1로 설정하면 된다 (예약메세지가 몇개나 들어올지 몰라.. 무슨값으로 잡아야 하나.. 부하 걸리면 이것도 큰일인데..)
	})
	if err != nil {
		log.Println("Error creating or updating consumer:", err)
		return
	}

	if _, err := consumer.Consume(func(msg jetstream.Msg) {
		scheduled_at_str := msg.Headers().Get("SCHEDULED-AT")

		if scheduled_at_str != "" {
			scheduled_at, err := time.Parse(time.RFC3339, scheduled_at_str)
			if err != nil {
				log.Println("Error parsing scheduled-at header:", err)
				return
			}

			delay := time.Until(scheduled_at)
			if delay > 0 {
				log.Printf("메시지 %s는 %v 후에 발송처리됩니다.", msg.Subject(), delay)
				msg.NakWithDelay(delay)
				return
			}
		}

		NC.Publish("starfruit.h00017.scheduled_msg.task.processed", msg.Data())

		msg.Ack()
	}); err != nil {
		log.Println("Error starting consumer:", err)
		return
	}

	select {}
}

func SetupTaskStream(js jetstream.JetStream) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      "H00017_SCHEDULED_MSG_TASK_STREAM",
		Subjects:  []string{"starfruit.h00017.scheduled_msg.task.*.pending"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		return nil, err
	}

	return stream, nil
}

// 실제로는 즉시발송 워크큐 스트림에서 딜레이되는 메세지 수를 줄이기 위해 5분이내로 발송해야 하는 메세지들만 셀렉해서 가지고 오도록.. 시간 범위를 최소화하는게 좋을것같다.
func GetTaskListFromDBAndInsertToTaskStream() {
	var idx int = 1

	for {
		scheduled_at := time.Now().Add(time.Second * 20).Format(time.RFC3339) // 20초 후에 발송될 예정

		NC.PublishMsg(&nats.Msg{
			Subject: fmt.Sprintf("starfruit.h00017.scheduled_msg.task.%d.pending", idx),
			Header: nats.Header{
				"SCHEDULED-AT": []string{scheduled_at},
			},
			Data: []byte(fmt.Sprintf("Task message %d, %s에 도착예정", idx, scheduled_at)),
		})
		idx++

		time.Sleep(2 * time.Second) // 2초마다 메시지 발행
	}

}

func ListenScheduledMsg() {
	NC.Subscribe("starfruit.h00017.scheduled_msg.task.processed", func(msg *nats.Msg) {
		log.Printf("예약 메세지를 받았어요: %s, 수신 시각: %v", string(msg.Data), time.Now())
		msg.Ack()
	})
}

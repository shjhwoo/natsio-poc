package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var NC *nats.Conn

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

	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      "h00017",
		Subjects:  []string{"h00017.>"},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour, // 최대 24시간 동안 메시지를 보관
	})
	if err != nil {
		log.Println("Error creating or updating stream:", err)
		return
	}

	for i := 0; i < 17; i++ {
		userId := fmt.Sprintf("user%d", i%3+1)
		status := getRandomStatus()

		subject := fmt.Sprintf("h00017.%s.status", userId)

		err := NC.Publish(subject, []byte(status))
		if err != nil {
			log.Println("Error publishing message:", err)
			return
		}

		log.Println("Published message to subject:", subject, "with status:", status)

		time.Sleep(500 * time.Millisecond)
	}

	//컨슈머를 만들어서 각 사용자에 대한 최신 상태를...가지고 온다

	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		FilterSubject: "h00017.*.status",
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Println("Error creating or updating consumer:", err)
		return
	}

	//스트림 정보 확인중..
	streamInfo := stream.CachedInfo()
	log.Println("Stream Info:", streamInfo.State.Msgs)

	if _, err := consumer.Consume(func(msg jetstream.Msg) {
		subject := msg.Subject()
		status := string(msg.Data())

		tokens := strings.Split(subject, ".")
		userId := tokens[1]

		log.Printf("User %s latest status : %s\n", userId, status)
	}); err != nil {
		log.Println("Error consuming messages:", err)
		return
	}

}

var statusList = []string{
	"offline",
	"online",
	"away",
}

func getRandomStatus() string {
	randomIndex := rand.Intn(len(statusList))
	return statusList[randomIndex]
}

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
		MaxAge:    20 * time.Second,
	})
	if err != nil {
		log.Println("Error creating or updating stream:", err)
		return
	}

	// consumerBeforeTimeOut, err := stream.Consumer(context.Background(), "status_consumer")
	// if err != nil {
	// 	log.Println("Error getting consumer before timeout:", err)
	// 	return
	// }

	// log.Println("Consumer before timeout:", consumerBeforeTimeOut)

	//컨슈머 만들어가지고
	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Name:              "status_consumer",
		FilterSubjects:    []string{"h00017.*.status"},
		InactiveThreshold: 15 * time.Second, // 비활성 상태로 5초 후에 자동으로 삭제
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		log.Println("Error creating or updating consumer:", err)
		return
	}
	log.Println("Consumer created successfully")

	for i := 0; i < 15; i++ {
		userId := fmt.Sprintf("user%d", i%3+1)
		status := getRandomStatus()

		subject := fmt.Sprintf("h00017.%s.status", userId)

		err := NC.Publish(subject, []byte(status))
		if err != nil {
			log.Println("Error publishing message:", err)
			return
		}

		log.Println("Published message to subject:", subject, "with status:", status)

		time.Sleep(1 * time.Second)
	}

	batchMessages, err := consumer.Fetch(5)
	if err != nil {
		log.Println("Error fetching messages:", err)
		return
	}

	for msg := range batchMessages.Messages() {
		mt, err := msg.Metadata()
		if err != nil {
			log.Println("Error getting message metadata:", err)
			return
		}

		log.Println("Received message:", string(msg.Data()), "on subject:", msg.Subject(), "sequence:", mt.Sequence, "timestamp:", mt.Timestamp)

		if err := msg.Ack(); err != nil {
			log.Println("Error acknowledging message:", err)
		} else {
			log.Println("Message acknowledged successfully")
		}
	}

	// if _, err := consumer.Consume(func(msg jetstream.Msg) {
	// 	mt, err := msg.Metadata()
	// 	if err != nil {
	// 		log.Println("Error getting message metadata:", err)
	// 		return
	// 	}

	// 	log.Println("Received message:", string(msg.Data()), "on subject:", msg.Subject(), "sequence:", mt.Sequence, "timestamp:", mt.Timestamp)

	// 	if err := msg.Ack(); err != nil {
	// 		log.Println("Error acknowledging message:", err)
	// 	} else {
	// 		log.Println("Message acknowledged successfully")
	// 	}
	// }); err != nil {
	// 	log.Println("Error starting consumer:", err)
	// 	return
	// }

	// log.Println("All messages published, waiting for consumer to process... 이제 남은거 다 빼내자")
	// cctx.Drain()

	w := 5
	time.Sleep(time.Duration(w) * time.Second)
	log.Printf("Waited for %d seconds before checking consumer...", w)

	consumerAfterTimeOut, err := stream.Consumer(context.Background(), "status_consumer")
	if err != nil {
		log.Println("Error getting consumer after timeout:", err)
		return
	}

	log.Println("Consumer after timeout:", consumerAfterTimeOut)

	reconnectedConsumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Name:              "status_consumer",
		FilterSubjects:    []string{"h00017.*.status"},
		InactiveThreshold: 15 * time.Second, // 비활성 상태로 5초 후에 자동으로 삭제
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       1,
		AckPolicy:         jetstream.AckExplicitPolicy,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		log.Println("Error creating or updating consumer:", err)
		return
	}

	batchMessages2, err := reconnectedConsumer.Fetch(5)
	if err != nil {
		log.Println("Error fetching messages:", err)
		return
	}

	for msg := range batchMessages2.Messages() {
		mt, err := msg.Metadata()
		if err != nil {
			log.Println("Error getting message metadata:", err)
			return
		}

		log.Println("Received message:", string(msg.Data()), "on subject:", msg.Subject(), "sequence:", mt.Sequence, "timestamp:", mt.Timestamp)

		if err := msg.Ack(); err != nil {
			log.Println("Error acknowledging message:", err)
		} else {
			log.Println("Message acknowledged successfully")
		}
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

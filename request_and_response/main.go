package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream" // Import jetstream package
)

const (
	natsURL        = "nats://127.0.0.1:4222" // Adjust if your NATS server is elsewhere
	requestSubject = "my.service.request"
	streamName     = "MY_SERVICE_STREAM"
	consumerName   = "my_service_consumer"
)

func main() {
	// --- 1. Connect to NATS and JetStream ---
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("Connected to NATS at %s", natsURL)

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Error connecting to JetStream: %v", err)
	}
	log.Println("Connected to JetStream")

	// --- 2. Configure and Create JetStream Stream ---
	// Add the request subject to the stream's subjects
	cfg := jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{requestSubject, "other.subjects.*"}, // Ensure requestSubject is included
		Storage:  jetstream.MemoryStorage,                      // Use MemoryStorage for simplicity, FileStorage for persistence
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := js.CreateOrUpdateStream(ctx, cfg)
	if err != nil {
		log.Fatalf("Error creating or updating stream: %v", err)
	}
	log.Printf("Stream '%s' created/updated with subjects: %v", stream.CachedInfo().Config.Name, stream.CachedInfo().Config.Subjects)

	// --- 3. Create a JetStream Consumer to read from the stream ---
	// This consumer will receive messages that are persisted to the stream.
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy, // Explicitly acknowledge messages
	})
	if err != nil {
		log.Fatalf("Error creating or updating consumer: %v", err)
	}
	log.Printf("Consumer '%s' created/updated for stream '%s'", consumer.CachedInfo().Name, streamName)

	// --- 4. Start the NATS Request Handler (normal subscriber) ---
	// This subscriber will respond to the nats.Request.
	// This handler *does not* interact with JetStream directly,
	// but its published message (the request) will be persisted by JetStream.
	_, err = nc.Subscribe(requestSubject, func(m *nats.Msg) {
		log.Printf("[Request Handler] Received request on subject '%s': %s", m.Subject, string(m.Data))
		// Respond to the request
		if err := m.Respond([]byte("Response from service!")); err != nil {
			log.Printf("[Request Handler] Error responding: %v", err)
		}
	})
	if err != nil {
		log.Fatalf("Error subscribing to request subject: %v", err)
	}
	log.Printf("NATS Request Handler subscribed to '%s'", requestSubject)

	// Give a moment for subscribers to be ready
	time.Sleep(1 * time.Second)

	// --- 5. Start a goroutine to consume messages from the JetStream stream ---
	// This demonstrates that the message sent via nats.Request is indeed
	// stored and can be consumed from the JetStream stream.
	go func() {
		log.Printf("[JetStream Consumer] Starting to consume messages from stream '%s', consumer '%s'", streamName, consumerName)
		// Use Fetch for pull consumers or consume context for push consumers
		// Here, using a simple loop with Fetch to pull messages
		for {
			msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				if err == jetstream.ErrNoMessages {
					// log.Println("[JetStream Consumer] No new messages, trying again...")
					continue
				}
				log.Printf("[JetStream Consumer] Error fetching messages: %v", err)
				return
			}

			for msg := range msgs.Messages() {
				log.Printf("[JetStream Consumer] JetStream received message on subject '%s': %s", msg.Subject(), string(msg.Data()))
				// Acknowledge the message to JetStream
				if err := msg.Ack(); err != nil {
					log.Printf("[JetStream Consumer] Error acknowledging message: %v", err)
				}
			}
		}
	}()

	// --- 6. Send a nats.Request ---
	log.Printf("\n[Client] Sending nats.Request to '%s'...", requestSubject)
	msg, err := nc.Request(requestSubject, []byte("Hello JetStream from Request!"), 5*time.Second)
	if err != nil {
		log.Fatalf("[Client] Error sending request: %v", err)
	}
	log.Printf("[Client] Received response to request: %s", string(msg.Data))

	// Keep the main goroutine alive for a bit to see the JetStream consumer work
	time.Sleep(5 * time.Second)

	// Optional: Clean up stream and consumer
	log.Println("\nCleaning up JetStream resources...")
	if err := js.DeleteConsumer(ctx, streamName, consumerName); err != nil {
		log.Printf("Error deleting consumer: %v", err)
	} else {
		log.Printf("Consumer '%s' deleted.", consumerName)
	}
	if err := js.DeleteStream(ctx, streamName); err != nil {
		log.Printf("Error deleting stream: %v", err)
	} else {
		log.Printf("Stream '%s' deleted.", streamName)
	}

	log.Println("Exiting.")
}

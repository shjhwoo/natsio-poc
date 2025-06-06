// main.go
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	const workerCount = 3
	const queueGroup = "task-workers"

	// ===== 여러 Worker 시작 =====
	for i := 1; i <= workerCount; i++ {
		workerID := i
		go func() {
			_, err := nc.QueueSubscribe("tasks", queueGroup, func(msg *nats.Msg) {
				log.Printf("[Worker #%d] 작업 수신: %s\n", workerID, string(msg.Data))

				// 처리 시뮬레이션
				time.Sleep(500 * time.Millisecond)

				// 응답 전송
				response := fmt.Sprintf("Worker #%d가 '%s' 처리함", workerID, msg.Data)
				_ = msg.Respond([]byte(response))
			})
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	// Subscriber 준비 시간 확보
	time.Sleep(1 * time.Second)

	// ===== Client 역할 =====
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task := fmt.Sprintf("작업 #%d", i)
			log.Println("[Client] 작업 요청:", task)

			// 요청 전송 후 응답 대기
			msg, err := nc.Request("tasks", []byte(task), 2*time.Second)
			if err != nil {
				log.Println("[Client] 응답 실패:", err)
				return
			}
			log.Println("[Client] 응답 수신:", string(msg.Data))
		}(i)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)
}

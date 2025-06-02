package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "mgrsol123",
	})

	for i := 0; i < 4; i++ {
		go func() {
			var j int

			now := time.Now()

			for {
				score := now.Add(5 * time.Second).Unix()
				rdb.ZAdd(ctx, "scheduled_msgs", redis.Z{
					Score:  float64(score),
					Member: fmt.Sprintf("msg-%d", j),
				})

				j++
			}
		}()
	}

	for i := 0; i < 4; i++ {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			for range ticker.C {
				msgs, err := rdb.ZPopMin(ctx, "scheduled_msgs", 50000).Result()
				if err != nil {
					fmt.Println("ZPOPMIN error:", err)
					continue
				}
				fmt.Printf("Got %d msgs\n", len(msgs))

				if len(msgs) == 0 {
					fmt.Println("No more messages to process.")
					continue
				}

				nowUnix := time.Now().Unix()
				var resendMsgs []redis.Z
				var readyMsgs []string

				for _, msg := range msgs {
					if int64(msg.Score) <= nowUnix {
						readyMsgs = append(readyMsgs, msg.Member.(string))
					} else {
						// 아직 시간이 안 된 메시지는 다시 ZADD
						resendMsgs = append(resendMsgs, redis.Z{
							Score:  msg.Score,
							Member: msg.Member,
						})
					}
				}

				// 되돌리기
				if len(resendMsgs) > 0 {
					rdb.ZAdd(ctx, "scheduled_msgs", resendMsgs...)
				}

				fmt.Printf("Processed: %d | Rescheduled: %d\n", len(readyMsgs), len(resendMsgs))
			}
		}()
	}

	select {}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
)

var Stream jetstream.Stream
var Rc *redis.Client
var LastWSActiveTime time.Time
var latest_buffered_msg Ws_buffered_msg

var WSConn *websocket.Conn
var User_id string

var Consumer_worker_map = make(map[string]*ConsumerCtx)
var Oldest_buffer_save_time time.Time

func main() {
	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo("starfruit", "mgrsol123"))
	if err != nil {
		log.Println("Error connecting to NATS server:", err)
		return
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Println("Error creating js api:", err)
		return
	}

	rc := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", "localhost", 6379),
		Password: "mgrsol123",
		DB:       0,
	})

	status, err := rc.Ping(context.Background()).Result()
	if err != nil {
		log.Println("Error connecting to Redis server:", err)
		return
	}
	if status == "PONG" {
		log.Println("Connected to Redis server successfully")
	}
	Rc = rc

	stream, err := SetupStream(js)
	if err != nil {
		log.Println("Error setting up stream:", err)
		return
	}
	Stream = stream

	// 웹소켓 서버 열어준다 (8080 포트)
	http.HandleFunc("/ws", wsHandler)
	go func() {
		log.Println("WebSocket server listening on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal("WebSocket server error:", err)
		}
	}()

	//메세지를 한쪽에서는 1초 간격으로 계속해서 만들어준다
	go sendChatMsgs(nc)

	select {}
}

func SetupStream(js jetstream.JetStream) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      "H00017_STREAM",
		Subjects:  []string{"starfruit.h00017.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    time.Duration(30) * time.Second, // 3 minutes
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func sendChatMsgs(nc *nats.Conn) {
	log.Println("메세지 2초 간격으로 보내는 중..")

	var cnt int

	for {
		err := nc.Publish("starfruit.h00017.chat", fmt.Appendf([]byte{}, "Hello, this is a chat message %d!", cnt))
		if err != nil {
			log.Println("Error publishing message:", err)
			return
		}

		cnt++

		time.Sleep(2 * time.Second) // 1 second interval
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 모든 오리진 허용 (보안이 필요하다면 여기서 제한)
	},
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	log.Println("New WebSocket connection established")

	WSConn = conn

	User_id = r.URL.Query().Get("user_id")

	go ReadAndWriteWsMsg()

	key := fmt.Sprintf("%s:%s", "MSG_BUFFER", User_id)
	data, err := Rc.LIndex(context.Background(), key, 0).Result()
	if err != nil && err != redis.Nil {
		log.Println("Error reading buffered message from Redis:", err)
		return
	}

	if data != "" && err != redis.Nil {
		if err := json.Unmarshal([]byte(data), &latest_buffered_msg); err != nil {
			log.Println("Error unmarshalling buffered message:", err)
			return
		}

		if err := SendBufferedMsgs(); err != nil && err != redis.Nil {
			log.Println("Error sending buffered messages:", err)
			return
		} //버퍼에 있는거 일단은 다..
	}

	//사용자 컨슈머 생성 로직
	consumer, err := Resume_consumer()
	if err != nil {
		log.Println("Error resuming consumer:", err)
		return
	}

	ctx := &ConsumerCtx{
		Consumer:      consumer,
		Shutdown_chan: make(chan struct{}),
	}

	go StartConsumer(ctx)
	Consumer_worker_map[User_id] = ctx
}

func ReadAndWriteWsMsg() {
	log.Println("웹소켓 메세지 읽기 및 쓰기 시작")
	for {

		if WSConn == nil && LastWSActiveTime.IsZero() {
			LastWSActiveTime = time.Now()
		}

		// 간단한 ping-pong 처리 또는 메시지 수신 테스트용
		_, msg, err := WSConn.ReadMessage()
		if err != nil {
			log.Println("WebSocket read error:", err)
			WSConn = nil
			break
		}
		log.Printf("Received message: %s", msg)

		err = WSConn.WriteMessage(websocket.TextMessage, []byte("pong"))
		if err != nil {
			log.Println("WebSocket write error:", err)
			break
		}
	}
}

func SendBufferedMsgs() error {
	key := fmt.Sprintf("%s:%s", "MSG_BUFFER", User_id)

	for {
		buffered_msg, err := Rc.RPop(context.Background(), key).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		if err == redis.Nil {
			log.Println("버퍼에 있는 모든 메세지를 다 보냈어요.")
			Oldest_buffer_save_time = time.Time{} // 버퍼 비우기
			break
		}

		if WSConn == nil {
			_, err := Rc.RPush(context.Background(), key, buffered_msg).Result()
			if err != nil {
				return err
			}

			if time.Since(LastWSActiveTime) >= 5*time.Second {
				return errors.New("client is not alive for 5 seconds, stopping buffered message sending")
			}

			buffered_msg_cnt, err := Rc.LLen(context.Background(), key).Result()
			if err != nil {
				return err
			}

			if buffered_msg_cnt >= 5 {
				_, err := Rc.LPop(context.Background(), key).Result()
				if err != nil {
					return err
				}
			}

			continue
		}

		WSConn.WriteMessage(websocket.TextMessage, []byte(buffered_msg))
		log.Println("Sent buffered message to client:", string(buffered_msg))

		var buffered_msg_data Ws_buffered_msg
		if err := json.Unmarshal([]byte(buffered_msg), &buffered_msg_data); err != nil {
			log.Println("Error unmarshalling buffered message:", err)
		}
	}

	return nil
}

type Ws_buffered_msg struct {
	Stream_seqno     uint64    `json:"seqno"`
	Event_content    string    `json:"event_content"`                                            //이벤트 내용. JSON string으로 전달됨
	Event_created_at time.Time `json:"event_created_at" example:"2024-12-31T23:00:32.472+09:00"` //이벤트가 발생한 시각 RFC3339
}

func Resume_consumer() (jetstream.Consumer, error) {
	var optStartSeq uint64
	now := time.Now() // 한국 시간으로 변환
	consumer_absent_duration := now.Sub(latest_buffered_msg.Event_created_at)

	fmt.Println("현재시각: ", now, "컨슈머가 마지막으로 메세지를 보낸 시각: ", latest_buffered_msg.Event_created_at)
	fmt.Printf("컨슈머가 종료된 지 %f초 만에 재 접속", consumer_absent_duration.Seconds())

	if consumer_absent_duration >= time.Duration(30)*time.Second {
		log.Println("stream 메세지 보관기간 지나서 재접속을 하는 경우")
		err := Send_fallback_url(WSConn)
		if err != nil {
			return nil, err
		}

		first_msg, err := Get_first_consumer_msg()
		if err != nil {
			return nil, err
		}

		optStartSeq = first_msg.Stream_seqno
		if first_msg.Timestamp.IsZero() { //NEW 스트림에 메세지가 진짜 하나도 없을 수도 있으니까
			optStartSeq = 1
		}
	} else {
		//스트림 메세지 사라지기 전에 다시 접근해서 보는경우
		log.Println("stream 메세지 보관기간이 아직 남아있지만")

		overbuffered_msg_cnt, err := Count_overbuffered_consumer_msg(latest_buffered_msg.Stream_seqno)
		if err != nil {
			return nil, err
		}

		if overbuffered_msg_cnt >= 10 {
			log.Println("WS 만으로 가지고 와야 하는 메세지 개수가 10개 이상인 경우")

			err := Send_fallback_url(WSConn)
			if err != nil {
				return nil, err
			}

			first_msg, err := Get_first_consumer_msg()
			if err != nil {
				return nil, err
			}

			optStartSeq = first_msg.Stream_seqno

		} else {
			log.Printf("WS 만으로 가지고 와야 하는 메세지 개수가 10개 미만인 경우: %d개", overbuffered_msg_cnt)

			optStartSeq = latest_buffered_msg.Stream_seqno + 1
		}
	}

	consumer, err := Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Name:           fmt.Sprintf("h00017_%s_consumer", User_id),
		FilterSubjects: []string{"starfruit.h00017.>"},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    optStartSeq,
		AckPolicy:      jetstream.AckExplicitPolicy,
		ReplayPolicy:   jetstream.ReplayInstantPolicy,
	})
	if err != nil {

		if errors.Is(err, jetstream.ErrConsumerExists) {
			log.Println("기존 컨슈머가 존재해서 삭제하고 새로 생성합니다:", User_id)
			err := Stream.DeleteConsumer(context.Background(), fmt.Sprintf("h00017_%s_consumer", User_id))
			if err != nil {
				log.Println("Error deleting existing consumer:", err)
				return nil, err
			}

			newConsumer, err := Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
				Name:           fmt.Sprintf("h00017_%s_consumer", User_id),
				FilterSubjects: []string{"starfruit.h00017.>"},
				DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:    optStartSeq,
				AckPolicy:      jetstream.AckExplicitPolicy,
				ReplayPolicy:   jetstream.ReplayInstantPolicy,
			})
			if err != nil {
				log.Println("Error deleting existing consumer:", err)
				return nil, err
			}

			return newConsumer, nil
		}

		return nil, err
	}

	return consumer, nil
}

func Send_fallback_url(conn *websocket.Conn) error {

	conn.WriteMessage(websocket.TextMessage, []byte("https://fallback-url.com"))

	log.Println("폴백 URL 전송 완료")
	return nil
}

type Consumer_msg struct {
	Timestamp      time.Time
	Stream_seqno   uint64
	Consumer_seqno uint64 //컨슈머 시퀀스 번호는 현재 사용하지 않음
	Data           []byte
}

func Get_first_consumer_msg() (Consumer_msg, error) {
	//임시 컨슈머 생성한 다음에 정보 얻고 바로 삭제 시키기
	consumer, err := Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		FilterSubjects: []string{"starfruit.h00017.>"},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return Consumer_msg{}, err
	}

	batch, err := consumer.FetchNoWait(1)
	if err != nil {
		return Consumer_msg{}, err
	}

	var consumerMsg Consumer_msg
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			return Consumer_msg{}, err
		}

		consumerMsg = Consumer_msg{
			Timestamp:      meta.Timestamp,
			Stream_seqno:   meta.Sequence.Stream,
			Consumer_seqno: meta.Sequence.Consumer,
			Data:           msg.Data(),
		}
	}

	if !consumerMsg.Timestamp.IsZero() {
		fmt.Println("오랜만에 접속해서 보는, stream의 제일 첫번째 메세지 시각", consumerMsg.Timestamp)
	}

	if err := Stream.DeleteConsumer(context.Background(), consumer.CachedInfo().Name); err != nil {
		return Consumer_msg{}, err
	}

	return consumerMsg, nil
}

func Count_overbuffered_consumer_msg(last_buffered_msg_seqno uint64) (uint64, error) {
	startCon, err := Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   last_buffered_msg_seqno + 1,
	})
	if err != nil {
		return 0, err
	}

	if err := Stream.DeleteConsumer(context.Background(), startCon.CachedInfo().Name); err != nil {
		return 0, err
	}

	return startCon.CachedInfo().NumPending, nil
}

type ConsumerCtx struct {
	Consumer      jetstream.Consumer
	Shutdown_chan chan struct{}
}

func StartConsumer(consumerCtx *ConsumerCtx) {
	for {
		select {
		case <-consumerCtx.Shutdown_chan:
			log.Println("ConsumerWorker shutting down:", User_id)

			Stream.DeleteConsumer(context.Background(), fmt.Sprintf("h00017_%s_consumer", User_id))
			delete(Consumer_worker_map, User_id)

			return

		default:
			batch, err := consumerCtx.Consumer.FetchNoWait(5)
			if err != nil {
				log.Println("Error fetching messages:", err)
				time.Sleep(500 * time.Millisecond) // backoff
				continue
			}

			for msg := range batch.Messages() {
				handleMsg(msg)
			}
		}
	}
}

func handleMsg(msg jetstream.Msg) {
	if WSConn != nil {

		WSConn.WriteMessage(websocket.TextMessage, msg.Data())

		return
	}

	log.Println("웹소켓 연결이 끊어졌어요. 버퍼에 저장을 시작합니다.")

	meta, err := msg.Metadata()
	if err != nil {
		log.Println("Error getting message metadata:", err)
		return
	}

	key := fmt.Sprintf("%s:%s", "MSG_BUFFER", User_id)

	fmt.Println("메세지 메타 타임스탬프 보기: ", meta.Timestamp)

	bytes, err := json.Marshal(Ws_buffered_msg{
		Stream_seqno:     meta.Sequence.Stream,
		Event_content:    string(msg.Data()),
		Event_created_at: meta.Timestamp,
	})
	if err != nil {
		log.Println("Error marshalling buffered message:", err)
		return
	}

	if _, err := Rc.LPush(context.Background(), key, string(bytes)).Result(); err != nil {
		log.Println("Error pushing message to Redis buffer:", err)
		return
	}
	msg.Ack()

	buffered_msg_cnt, err := Rc.LLen(context.Background(), key).Result()
	if err != nil {
		log.Println("Error getting buffered message count:", err)
		return
	}

	if buffered_msg_cnt == 1 && Oldest_buffer_save_time.IsZero() {
		// 첫 버퍼 메세지 저장 시각을 기록
		Oldest_buffer_save_time = time.Now() // 한국 시간으로 변환
		log.Println("첫 버퍼 메세지 저장 시각:", Oldest_buffer_save_time)
	}

	if buffered_msg_cnt >= 5 || time.Since(Oldest_buffer_save_time) >= 15*time.Second {
		log.Printf("버퍼에 저장된 메세지가 %d개 OR 가장 오래된 버퍼 메세지 저장 후 %v초가 지나서, 컨슈머를 종료해야 합니다", buffered_msg_cnt, time.Since(Oldest_buffer_save_time).Seconds())

		close(Consumer_worker_map[User_id].Shutdown_chan)
	}
}

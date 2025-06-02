package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// 마지막 메세지 관련 변수
var lastSeqNo uint64
var lastActiveAt time.Time

func main() {
	url := SetWsUrl()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	log.Println("Connected to WebSocket server")

	// 종료 시그널 처리용 채널
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	conn.SetPingHandler(func(appData string) error {
		log.Println("Received ping:", appData)
		// 핑 응답을 보내는 경우
		return conn.WriteMessage(websocket.PongMessage, []byte("pong"))
	})

	done := make(chan struct{})
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("Read error:", err)
				break
			}
			log.Printf("Received: %s", message)

			var parsed WsMsg
			if err := json.Unmarshal(message, &parsed); err != nil {
				log.Printf("JSON 파싱 오류: %v", err)
				continue
			}

			lastSeqNo = parsed.SeqNo
			lastActiveAt = time.Now()
		}
		close(done)
	}()

	// 시그널 수신 대기
	select {
	case <-sigChan:
		log.Println("Interrupt signal received. Cleaning up...")
	case <-done:
		log.Println("WebSocket connection closed.")
	}

	SaveUserAccessLog()
}

type WsMsg struct {
	SeqNo uint64 `json:"seqno"`
}

type LocalStorage struct {
	LastSeqNo    uint64    `json:"last_seqno"`
	LastActiveAt time.Time `json:"last_active_at"` // RFC3339 포맷 (string)
}

func SetWsUrl() string {

	// localstorage.json 파일이 존재하면 읽기
	file, err := os.Open("./localstorage.json")
	if err == nil {
		defer file.Close()

		var storage LocalStorage
		if err := json.NewDecoder(file).Decode(&storage); err != nil {
			log.Printf("JSON 디코딩 오류: %v", err)
		} else {
			lastSeqNo = storage.LastSeqNo
			lastActiveAt = storage.LastActiveAt
		}
	} else {
		log.Println("localstorage.json 파일 없음. 기본값 사용.")
	}

	url := fmt.Sprintf(
		"ws://localhost:8080/ws?user_id=U123&last_seqno=%d&last_access=%s",
		lastSeqNo,
		url.QueryEscape(lastActiveAt.Format(time.RFC3339)), // RFC3339 포맷 (string)
	)

	return url
}

func SaveUserAccessLog() {
	storage := LocalStorage{
		LastSeqNo:    lastSeqNo,
		LastActiveAt: lastActiveAt,
	}

	fmt.Println("storage:", storage)

	file, err := os.Create("./localstorage.json")
	if err != nil {
		log.Printf("파일 생성 실패: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // 보기 좋게 들여쓰기
	if err := encoder.Encode(storage); err != nil {
		log.Printf("JSON 인코딩 실패: %v", err)
		return
	}

	log.Println("localstorage.json 파일에 저장 완료")
}

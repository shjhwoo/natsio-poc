package main

import (
	"log"

	"github.com/gorilla/websocket"
)

func main() {

	// 서버 연결 시도
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws?user_id=U123", nil)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	log.Println("Connected to WebSocket server")

	conn.SetPingHandler(func(appData string) error {
		log.Println("Received ping:", appData)
		// 핑 응답을 보내는 경우
		return conn.WriteMessage(websocket.PongMessage, []byte("pong"))
	})

	// 메시지 계속 읽기
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			break
		}
		log.Printf("Received: %s", message)
	}

}

//환경에 따른 테스트

//연결을 했다가 5초 뒤에 끊었다가 다시 재 연결..

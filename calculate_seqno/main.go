package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// GetFirstStreamSeqno는 스트림의 가장 오래된 메시지 시퀀스 번호를 반환합니다.
func GetFirstStreamSeqno(js jetstream.JetStream, streamName string) (uint64, error) {
	stream, err := js.Stream(context.Background(), streamName)
	if err != nil {
		return 0, err
	}

	info, err := stream.Info(context.Background())
	if err != nil {
		return 0, err
	}

	return info.State.FirstSeq, nil
}

func main() {
	// NATS 서버에 연결
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("NATS 연결 실패: %v", err)
	}
	defer nc.Close()

	// JetStream 컨텍스트 생성
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("JetStream 컨텍스트 생성 실패: %v", err)
	}

	streamName := "TEST_STREAM_TTL"
	subjectName := "test.subject"

	// 기존 스트림이 있다면 삭제합니다.
	js.DeleteStream(context.Background(), streamName)

	// MaxAge(TTL)를 5초로 설정하여 스트림을 생성합니다.
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subjectName},
		MaxAge:   5 * time.Second,
	})
	if err != nil {
		log.Fatalf("스트림 생성 실패: %v", err)
	}
	log.Printf("'%s' 스트림이 TTL 5초로 생성되었습니다.\n", streamName)

	// 10개의 메시지를 발행합니다.
	for i := 1; i <= 10; i++ {
		_, err := js.Publish(context.Background(), subjectName, []byte(fmt.Sprintf("메시지 #%d", i)))
		if err != nil {
			log.Fatalf("메시지 발행 실패: %v", err)
		}
		log.Printf("메시지 #%d 발행됨\n", i)
	}

	// 첫 번째 메시지의 시퀀스 번호를 가져옵니다. (TTL로 삭제되기 전)
	firstSeq, err := GetFirstStreamSeqno(js, streamName)
	if err != nil {
		log.Fatalf("FirstSeq 조회 실패: %v", err)
	}
	log.Printf("첫 번째 메시지 시퀀스 번호: %d\n", firstSeq) // 예상: 1

	log.Println("5초 대기 중...")
	// 5초 이상 대기하여 오래된 메시지가 TTL로 인해 삭제되도록 합니다.
	time.Sleep(6 * time.Second)

	// 오래된 메시지가 삭제된 후, 새로운 메시지를 발행하여 purge를 유도합니다.
	js.Publish(context.Background(), subjectName, []byte("새로운 메시지"))
	log.Println("새 메시지 발행. 오래된 메시지 삭제 유도.")

	// 다시 첫 번째 메시지의 시퀀스 번호를 가져옵니다.
	firstSeqAfterPurge, err := GetFirstStreamSeqno(js, streamName)
	if err != nil {
		log.Fatalf("FirstSeq 조회 실패: %v", err)
	}

	log.Printf("TTL 후 첫 번째 메시지 시퀀스 번호: %d\n", firstSeqAfterPurge) // 예상: 1보다 큰 값

}

## 스케줄러 스트림 부하테스트 

## 테스트 방법
```
go run main.go {보내려는 총 메세지 개수} {각 메세지별로 ScheduledAt을 수정할 횟수} {예약시각 간격} {예약시각당 보낼 메세지 개수}
```

예를 들어 총 2000개의 메세지를 각 메세지마다 10번 수정요청을 스케줄러 스트림에 보내고, 
1분 예약 시각마다 100게의 예약 메세지를 걸어두고 싶다면, 

go run main.go 2000 10 1 100 


배포된 호스트 환경에서는 start.sh 파일의 "go run main.go 2000 10 1 100" 라인을 테스트 조건에 맞게 수정 후 사용하세요. 
```
stopNats.sh
startNats.sh
start.sh
```
테스트 진행 후에는 그 결과를 topResult.log, loadResult.log로 확인 가능합니다. 

topResult.log - nats 서버 각 노드에 걸리는 cpu, mem 사용량을 30초 간격으로 확인
loadResult.log - 스케줄러 JS에 걸리는 cpu, mem 사용량을 30초 간격으로 확인


## 실제 성능 측정 결과 
```
[분당 2만건] 

제일 빠른 예약 시각: 2025-12-09 13:40:00.152268178 +0900 KST
제일 늦은 예약 시각: 2025-12-09 13:41:00.873926261 +0900 KST
평균 latency:  22.925588073s 
최대 latency: 51.563163004s

** latency:  scheduler stream으로부터 발행된 메세지를  subscriber가 수신한 시각 - ScheduledAt
** host: Rocky linux


[분당 100건]
(2000개의 메세지를 각 메세지마다 10번 수정요청을 스케줄러 스트림에 보내고, 
1분 예약 시각마다 100게의 예약 메세지를 전송)

avgLatency:  26.977μs maxLatency 10.168423ms count:  1998

curl /varz (CPU/Mem):

"cpu": 1 (CPU 사용률 1% 미만)

"mem": 59867136 (약 57MB 메모리 사용)

top (NATS 프로세스):

%CPU: 모든 nats-server 인스턴스가 **0.0%**를 기록했습니다.

%MEM: 모든 인스턴스가 0.3% ~ 0.4% 내외의 매우 낮은 메모리 사용률을 보였습니다.

시스템 전체 CPU: %Cpu(s)의 100.0 id는 **시스템이 100% 유휴 상태(Idle)**임을 나타냅니다.

[분당 200건]
(2000개의 메세지를 각 메세지마다 10번 수정요청을 스케줄러 스트림에 보내고, 
1분 예약 시각마다 200게의 예약 메세지를 전송)

avgLatency:  299.605μs maxLatency 51.031452ms count:  2000

curl /varz (CPU/Mem):

"cpu": 1 (CPU 사용률 1% 미만)

"mem": 60731392

[분당 500건]
(2000개의 메세지를 각 메세지마다 10번 수정요청을 스케줄러 스트림에 보내고, 
1분 예약 시각마다 500게의 예약 메세지를 전송)
avgLatency:  61.031855ms maxLatency 577.703056ms count:  1998

curl /varz (CPU/Mem):

"cpu": 1 (CPU 사용률 1% 미만)

"mem": 56750080
```
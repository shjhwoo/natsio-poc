# Scheduler stream Msg 생성, 수정, 삭제

* scheduler stream config 설명 

- nats server 2.12.2 이상 버전부터 지원합니다 
- AllowMsgSchedules, AllowMsgTTL는 필수속성입니다
- 스케줄러 목적으로 생성한 Jetstream의 AllowMsgSchedules, AllowMsgTTL는 다시 false로 되돌릴 수 없다 
- 일반 stream과 마찬가지로 consumer를 여럿 생성하고, 그 consumer들이 fanout 방식으로 예약시각에 메세지를 받아볼 수 있습니다. 단, 이때 consumer는 반드시 CreatePushConsumer 함수를 사용하여 생성해야합니다. (getScheduledMessageFromConsumer함수 참고)
- 일반 stream과 마찬가지로 RePublish 속성과 함께 사용 가능합니다. 

* 헤더 설명
- Nats-Schedule : 희망 예약 시각, RFC3339 형식으로 기재합니다.
 현재 시각으로도, 과거 시각으로도 설정 가능합니다. 
 이 경우는 Scheduler stream에 메세지를 발행하자마자 Nats-Schedule-Target으로 메세지가 새로 발행됩니다
 (즉시발송과 같은 개념)

- Nats-Schedule-TTL : 예약 시각에 발행 후에 해당 메세지를 Stream에 얼마나 오래 보관할 것인지를 결정합니다. 
단위는 m(분), s(초) 중 선택해서 설정 가능합니다. 
0으로 설정 불가합니다. 
해당 값을 제대로 지정하지 않게 되면, Nats-Schedule 값이 과거 시각인 경우 원하지 않게 메세지가 발행이 즉시 되어버릴 수 있으니 주의해야 합니다.

- Nats-Schedule-Target : 예약 시각에 발행된 메세지는 Scheduler stream에 다시 저장이 됩니다. 
Scheduler stream에 지정한 Subject 중에 하나의 Subject를 이 헤더의 값으로 지정하게 되면, 예약 시각에 해당 Subject로 메세지가 발행되며 Scheduler stream에 저장됩니다. 해당 헤더 값은 위 두 헤더의 값과 함께 반드시 같이 지정해야 합니다

- 이외에 내가 원하는 헤더들을 추가로 커스텀해서 지정할 수 있으며 해당 헤더들은 지정된 예약 시각에 발행되는 메세지에 함께 포함이 됩니다. 

* 생성 
- Nats-Schedule, Nats-Schedule-TTL, Nats-Schedule-Target 헤더를 필수로 사용하여 메세지를 발행합니다. 

* 수정
- scheduler stream config: MaxMsgsPerSubject = 1, Discard = DiscardOld, Retention = LimitsPolicy 조건을 충족해야 합니다.
 (같은 Subject로 메세지가 연속으로 들어오는 경우엔 가장 최근의 메세지만 stream에 저장하겠다는 의미입니다)
- 생성과 동일한 헤더를 사용합니다. 
- overwite 하는 방식으로 이루어집니다. 

* 삭제
- scheduler stream config: MaxMsgsPerSubject = 1, Discard = DiscardOld, Retention = LimitsPolicy 조건을 충족해야 합니다.
 (같은 Subject로 메세지가 연속으로 들어오는 경우엔 가장 최근의 메세지만 stream에 저장하겠다는 의미입니다)
- 수정과 동일한 방법으로 이루어집니다. 단 Nats-Schedule의 값을 삭제를 요청한 현재 시각으로 지정을 하고, 
내가 원하는 특정한 이벤트 핸들러가 작동하지 않도록 메세지 헤더 속성을 변경하는 것으로 구현 전략을 생각해볼 수 있습니다. 

- 예제 코드에서는 Nats-Schedule-Target 값을 scheduler.onProcess로 지정하는 경우 예약 시각에 메세지가 QueueSubscriber들에게 전달되게끔 했는데, 
삭제 처리를 하게 되는 경우에는  Nats-Schedule-Target 값을 scheduler.discarded로 지정하여 해당 구독자들에게 메세지가 전달되지 않게했습니다. 


[Ref]
1. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md
2. https://nats.io/blog/nats-server-2.12-release/#delayed-message-scheduling


## 실행방법

docker-compose up -d 

go run main.go
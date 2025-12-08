# Scheduler 

[목적]
Publish a message at a later time
Regularly publish a message on a schedule
Publish the latest message for a subject on a schedule, to be used for data sampling

[주의사항]
스케줄러 목적으로 생성한 Jetstream의 AllowMsgSchedules, AllowMsgTTL는 다시 false로 되돌릴 수 없다 



[Ref]
1. https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md
2. https://nats.io/blog/nats-server-2.12-release/#delayed-message-scheduling


## 실행방법

docker-compose up -d 

go run main.go
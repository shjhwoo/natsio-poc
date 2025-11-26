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


첨부한 문서 1번의 내용이 실제 사용중인 2.12 버전 서버엣서는 미지원 되어서 해당 기능 사용이 불가했습니다.
https://github.com/nats-io/nats-server/blob/e3dcbfd7c2655ac598697871416d8ca082e62d84/server/stream.go#L4874
https://github.com/nats-io/nats-server/blob/e3dcbfd7c2655ac598697871416d8ca082e62d84/server/stream.go#L5670
https://github.com/nats-io/nats-server/blob/9831cbc4314cc00bbe196021e47e240ad0c2b5f3/server/jetstream_errors_generated.go#L1937
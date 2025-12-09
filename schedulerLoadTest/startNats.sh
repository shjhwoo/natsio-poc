#!/bin/bash

echo "?? NATS Server 인스턴스 3개 실행 시작 (백그라운드)"

# nohup을 사용하여 세션이 끊겨도 실행 유지
# > log 2>&1 : 표준 출력과 표준 에러를 로그 파일로 리디렉션
nohup nats-server -c s1.conf > s1.log 2>&1 &
echo "   - Server 1 실행 완료 (PID: $!, Log: s1.log)"

nohup nats-server -c s2.conf > s2.log 2>&1 &
echo "   - Server 2 실행 완료 (PID: $!, Log: s2.log)"

nohup nats-server -c s3.conf > s3.log 2>&1 &
echo "   - Server 3 실행 완료 (PID: $!, Log: s3.log)"

# NATS 서버가 완전히 시작될 시간을 잠시 대기
sleep 5
echo "=============== started NATS server ==============="
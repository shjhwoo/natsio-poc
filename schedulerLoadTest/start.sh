#!/bin/bash

# JSON 파싱 도구 'jq'가 설치되어 있는지 확인
if ! command -v jq &> /dev/null
then
    echo "?? Error: jq is required for JSON parsing. Please install it (e.g., sudo dnf install jq)."
    exit 1
fi

# ----------------------------------------------------
# 1. NATS 서버 PID 목록 가져오기
# ----------------------------------------------------
get_nats_pids() {
    # 4222, 4223, 4224 포트로 실행 중인 NATS 서버의 PID를 추출
    PIDS=$(ss -tlnup | grep -E ':(4222|4223|4224)' | awk -F'pid=' '{print $2}' | awk -F',' '{print $1}' | tr '\n' ' ')
    echo $PIDS
}

# ----------------------------------------------------
# 2. TOP 명령어 모니터링 함수 (topResult.log 기록)
# ----------------------------------------------------
monitor_top() {
    local LOG_FILE="topResult.log"
    echo "=== NATS Server TOP Monitoring Started: $(date) ===" > "$LOG_FILE"

    while true; do
        PIDS=$(get_nats_pids)
        if [ -z "$PIDS" ]; then
            echo "$(date): Warning: NATS PIDs not found. Monitoring paused." >> "$LOG_FILE"
            sleep 30
            continue
        fi

        echo "--- $(date) ---" >> "$LOG_FILE"
        # -b: 배치 모드, -n 1: 1회 실행, -p: 지정된 PID만 모니터링
        top -b -n 1 -p $(echo $PIDS | tr ' ' ',') >> "$LOG_FILE"

        # 30초 대기
        sleep 30
    done
}

# ----------------------------------------------------
# 3. CURL JSZ 모니터링 함수 (loadResult.log 기록)
# ----------------------------------------------------
monitor_curl_jsz() {
    local LOG_FILE="loadResult.log"
    echo "=== NATS JSZ Monitoring Started: $(date) ===" > "$LOG_FILE"

    while true; do
        echo "--- $(date) ---" >> "$LOG_FILE"

        # curl 요청을 보내고 jq를 사용하여 memory 및 cpu 정보만 추출하여 로그 파일에 추가
        curl -s http://localhost:8222/varz?js=true | jq '{mem: .mem, cpu: .cpu}' >> "$LOG_FILE"

        # 30초 대기
        sleep 30
    done
}

# ----------------------------------------------------
# 4. 메인 실행 로직
# ----------------------------------------------------

# 백그라운드 모니터링 시작
monitor_top &
TOP_MONITOR_PID=$!
echo "TOP Monitor started with PID: $TOP_MONITOR_PID (Logging to topResult.log)"

monitor_curl_jsz &
CURL_MONITOR_PID=$!
echo "CURL JSZ Monitor started with PID: $CURL_MONITOR_PID (Logging to loadResult.log)"

echo "----------------------------------------"
echo "?? Load Test 실행 중..."
# Load Test 프로그램 실행 (이 명령어가 완료될 때까지 대기)
go run main.go 2000 10 1 100
LOAD_TEST_EXIT_CODE=$?
echo "? Load Test 완료 (종료 코드: $LOAD_TEST_EXIT_CODE)"
echo "----------------------------------------"

# 백그라운드 모니터링 프로세스 종료
echo "?? Monitoring 프로세스 종료 중..."
kill $TOP_MONITOR_PID $CURL_MONITOR_PID 2>/dev/null

echo "========================================="

exit $LOAD_TEST_EXIT_CODE

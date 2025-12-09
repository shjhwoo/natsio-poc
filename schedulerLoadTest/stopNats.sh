#!/bin/bash

# NATS Server 프로세스 (422x 포트)를 찾아 모두 종료하는 함수
kill_nats() {
    echo "========================================="
    echo "?? NATS Server 프로세스 종료 시도 (포트 422x)"

    # ss 명령어로 PID를 추출합니다.
    # -E : grep에서 정규표현식 사용 (4222, 4223, 4224, 4225 등)
    # awk를 사용하여 pid=XXX, 문자열에서 PID(XXX)만 추출
    PIDS=$(ss -tlnup | grep -E ':422[2-9]|:422[0-1]' | awk -F'pid=' '{print $2}' | awk -F',' '{print $1}')

    if [ -z "$PIDS" ]; then
        echo "? 실행 중인 NATS Server 프로세스가 없습니다."
    else
        echo "킬 할 PID 목록: $PIDS"
        # 추출된 PID를 xargs를 통해 kill 명령어에 전달
        # 2>/dev/null : 오류 메시지(예: 이미 종료됨)는 출력하지 않음
        echo "$PIDS" | xargs sudo kill -9 2>/dev/null

        # 종료 확인을 위해 잠시 대기
        sleep 2

        if ss -tlnup | grep -q -E ':422[2-9]|:422[0-1]'; then
            echo "?? 경고: 일부 NATS 프로세스가 여전히 실행 중일 수 있습니다."
        else
            echo "? 모든 NATS Server가 성공적으로 종료되었습니다."
        fi
    fi
    echo "========================================="
}

# NATS 프로세스 찾아서 모두 kill (테스트 후 정리)
kill_nats

rm -rf s*.log
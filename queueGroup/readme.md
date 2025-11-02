# Queue group

## description

하나의 subject(starfruit.internal.event)를 여러 개의 queue group이 구독하는 모델입니다.

3가지의 서비스가 있으며 각 서비스 별로 2개의 인스턴스가 있는 분산 환경 구성입니다.

각 서비스는 nats queue group(QueueSubscribe)을 이루고 있기 때문에
이벤트 수신 시, 서비스 내에서 단 하나의 인스턴스만이 이벤트를 처리합니다.

## 적용 사례 관련 문서

https://www.notion.so/15ab87aa3ee080e6a105ce72ecda2e3f?source=copy_link#27db87aa3ee08080af29fd7ba82c7d27

## 실행방법

```
# nats 서버 실행
docker-compose up -d

# poc 코드 실행
go run main.go
```

- [처리 완료] 로그에서 이벤트가 어떤 서비스의 어떤 인스턴스에 의해 처리가 되었는지 확인해 보세요
- 이벤트는 모든 서비스에 골고루 전달되어야 하지만, 각 서비스 내의 인스턴스 하나에만 전달되어 처리되는지 확인해 보세요

## 실행 결과 예시

```
PS C:\Users\samsung\Desktop\starfruit-nats-lab\queueGroup> docker-compose up -d
time="2025-11-02T14:27:11+09:00" level=warning msg="C:\\Users\\samsung\\Desktop\\starfruit-nats-lab\\queueGroup\\docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
[+] Running 2/2
 ✔ Network queuegroup_default                Created                                                                                                                                                   0.1s
 ✔ Container queuegroup-nats-hub-server-1-1  Started                                                                                                                                                   1.1s
PS C:\Users\samsung\Desktop\starfruit-nats-lab\queueGroup> go run .\main.go
2025/11/02 14:27:18 --- NATS Queue Group POC start ---
2025/11/02 14:27:18 NATS 서버 연결 성공
2025/11/02 14:27:18 총 3개 서비스 (3개 큐 그룹), 각 서비스당 2개 인스턴스 실행 중...
2025/11/02 14:27:19
>>> 메시지 발행 시작: 15개 이벤트를 'starfruit.internal.event' 주제로 전송
2025/11/02 14:27:19 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E001 | Time: 60ms
2025/11/02 14:27:19 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E001 | Time: 62ms
2025/11/02 14:27:19 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E001 | Time: 62ms
2025/11/02 14:27:19 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E002 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E002 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E002 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E003 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E003 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E003 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E004 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E004 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E004 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-1 | Queue Group: QueueGroupB | Data: Event ID: E005 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E005 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E005 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E006 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E006 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E006 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E007 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E007 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E007 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-1 | Queue Group: QueueGroupB | Data: Event ID: E008 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E008 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E008 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-1 | Queue Group: QueueGroupB | Data: Event ID: E009 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E009 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E009 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E010 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E010 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E010 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-1 | Queue Group: QueueGroupB | Data: Event ID: E011 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E011 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E011 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-1 | Queue Group: QueueGroupB | Data: Event ID: E012 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E012 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-2 | Queue Group: QueueGroupC | Data: Event ID: E013 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-2 | Queue Group: QueueGroupA | Data: Event ID: E013 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E013 | Time: 62ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E012 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E014 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E014 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E014 | Time: 62ms
2025/11/02 14:27:20 <<< 메시지 발행 완료
2025/11/02 14:27:20 [처리 완료] Worker: serviceA-1 | Queue Group: QueueGroupA | Data: Event ID: E015 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceC-1 | Queue Group: QueueGroupC | Data: Event ID: E015 | Time: 60ms
2025/11/02 14:27:20 [처리 완료] Worker: serviceB-2 | Queue Group: QueueGroupB | Data: Event ID: E015 | Time: 62ms
2025/11/02 14:27:21
--- POC 종료 (총 45개 작업 완료) ---
```

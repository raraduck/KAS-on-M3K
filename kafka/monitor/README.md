# Kafka 모니터링 실습 가이드

이 가이드는 Kafka 다중 브로커 환경에서 Prometheus와 Grafana를 활용한 모니터링 시스템 구축 및 사용 방법을 안내합니다.

## 목차

1. [환경 구성](#1-환경-구성)
2. [모니터링 시스템 접속](#2-모니터링-시스템-접속)
3. [테스트 토픽 생성](#3-테스트-토픽-생성)
4. [Consumer Lag 시뮬레이션](#4-consumer-lag-시뮬레이션)
5. [핵심 모니터링 지표](#5-핵심-모니터링-지표)
6. [알림 설정](#6-알림-설정)
7. [커스텀 Grafana 대시보드 생성](#7-커스텀-grafana-대시보드-생성)
8. [문제 해결](#8-문제-해결)

---

## 1. 환경 구성

### 1.1 시스템 요구사항
- Docker 및 Docker Compose 설치
- Python 3.6 이상 (confluent-kafka 패키지 필요)

### 1.2 실습 환경 시작

```bash
# 프로젝트 디렉토리로 이동
cd /path/to/your/kafka-docker-envs/kafka-monitoring

# 실행 권한 추가
chmod +x *.sh

# Docker Compose로 환경 시작
docker-compose up -d
```

### 1.3 환경 상태 확인

```bash
# 컨테이너 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs -f kafka-exporter
```

모든 컨테이너가 정상적으로 실행 중인지 확인해주세요.

---

## 2. 모니터링 시스템 접속

### 2.1 Kafka UI 접속

브라우저에서 [http://localhost:8080](http://localhost:8080)으로 접속하여 Kafka UI를 통해 브로커, 토픽, 파티션 상태를 확인할 수 있습니다.

### 2.2 Prometheus 접속

브라우저에서 [http://localhost:9090](http://localhost:9090)으로 접속하여 Prometheus 대시보드를 확인할 수 있습니다.

```
# 메트릭 쿼리 예시
kafka_topic_partition_current_offset
kafka_consumergroup_lag
```

### 2.3 Grafana 접속

브라우저에서 [http://localhost:3000](http://localhost:3000)으로 접속하여 Grafana 대시보드를 확인할 수 있습니다.

- 기본 계정: admin / admin

---

## 3. 테스트 토픽 생성

### 3.1 테스트 토픽 생성

```bash
# 3개 파티션, 3개 복제 팩터를 가진 테스트용 토픽 생성
./create_topic.sh test-lag 3 3
```

### 3.2 토픽 확인

```bash
# 토픽 정보 확인
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic test-lag
```

---

## 4. Consumer Lag 시뮬레이션

### 4.1 필요한 Python 패키지 설치

```bash
# confluent-kafka 패키지 설치 (로컬 환경에서 실행 시)
pip install confluent-kafka
```

### 4.2 메시지 프로듀서 실행

```bash
# 초당 1000개 메시지를 생성하는 프로듀서 실행
python producer_example.py --topic test-lag --rate 1000 --messages 100000
```

### 4.3 느린 컨슈머 실행

다른 터미널에서:

```bash
# 의도적으로 느린 컨슈머 실행
python consumer_example.py --topic test-lag --group slow-consumer --process-time 0.01
```

### 4.4 Consumer Lag 관찰

Grafana 또는 Prometheus에서 consumer lag 지표를 관찰합니다.

```
# Prometheus 쿼리 예시
sum(kafka_consumergroup_lag{consumergroup="slow-consumer"}) by (consumergroup)
```

---

## 5. 핵심 모니터링 지표

### 5.1 브로커 관련 지표

- **Under-replicated 파티션**: `kafka_topic_partition_under_replicated_partition`
  - 0보다 크면 복제 지연 또는 브로커 장애 가능성
  
### 5.2 토픽 및 파티션 지표

- **메시지 입력 속도**: `rate(kafka_topic_partition_current_offset[5m])`
  - 토픽별, 파티션별 메시지 처리량 모니터링
  
### 5.3 컨슈머 그룹 지표

- **Consumer Lag**: `kafka_consumergroup_lag`
  - 컨슈머 처리 지연 모니터링
  - 예시: `sum(kafka_consumergroup_lag) by (consumergroup, topic)`
  
- **Consumer 상태**: `kafka_consumergroup_members`
  - 활성 컨슈머 수 모니터링

---

## 6. 알림 설정

### 6.1 Grafana 알림 설정

1. Grafana 대시보드에서 설정 아이콘 클릭
2. "Alerting" 메뉴 선택
3. "New alert rule" 설정
4. 새 알림 규칙 생성:
   - Consumer Lag이 10,000 이상이면 경고
   - Under-replicated 파티션이 존재하면 경고

### 6.2 알림 테스트

1. 프로듀서 속도를 높이거나 컨슈머를 중지하여 lag 증가 테스트
2. 브로커를 중지하여 under-replicated 파티션 발생 테스트:
   ```bash
   docker stop kafka3
   ```

---

## 7. 커스텀 Grafana 대시보드 생성

Grafana에서 Kafka 모니터링을 위한 커스텀 대시보드를 생성하는 방법을 단계별로 안내합니다.

### 7.1 새 대시보드 생성

1. 웹 브라우저에서 Grafana에 접속합니다: http://localhost:3000
2. 로그인합니다 (기본 계정: admin / admin)
3. 왼쪽 메뉴에서 "+" 아이콘을 클릭하고 "Dashboard"를 선택합니다
4. "new dashboard" 버튼을 클릭합니다
  * Dashboard: 여러 개의 "패널"이 모여있는 전체 화면
  * Panel: 그래프, 테이블, 싱글 스탯(숫자 하나 표시), 차트 등을 나타내는 작은 단위
    - 패널 하나가 "쿼리+시각화"를 담당

### 7.2 브로커별 상태 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. 첫 번째 패널에서 "Prometheus" 데이터 소스를 선택합니다
3. 쿼리 필드에 다음을 입력합니다:
   ```
   kafka_brokers
   ```
4. 패널 타이틀을 "브로커 상태"로 설정합니다
5. "save" 버튼 하단의 "Visualization" 설정에서 "Stat" 형태를 선택합니다
6. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.3 Under-replicated 파티션 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. builder -> code로 변경합니다.
3. 쿼리 필드에 다음을 입력합니다:
   ```
   sum(kafka_topic_partition_under_replicated_partition) by (topic)
   ```
4. 패널 타이틀을 "Under-replicated 파티션"으로 설정합니다
5. "Visualization" 설정에서 "Table" 형태를 선택합니다
6. legend 옵션에서 format을 "Table"으로 설정합니다
7. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.4 토픽별 메시지 처리량 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. builder -> code로 변경합니다.
3. 쿼리 필드에 다음을 입력합니다:
   ```
   sum(rate(kafka_topic_partition_current_offset[5m])) by (topic)
   ```
4. 패널 타이틀을 "토픽별 메시지 처리량 (메시지/초)"로 설정합니다
5. "Visualization" 설정에서 "Time series" 형태를 선택합니다
6. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.5 파티션별 오프셋 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. 쿼리 필드에 다음을 입력합니다:
   ```
   kafka_topic_partition_current_offset
   ```
3. 패널 타이틀을 "파티션별 현재 오프셋"으로 설정합니다
4. "Visualization" 설정에서 "Table" 형태를 선택합니다
5. 테이블 설정에서 "Column:topic", "Column:partition", "Column:Value"가 보이도록 설정합니다
6. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.6 컨슈머 그룹별 Lag 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. 쿼리 필드에 다음을 입력합니다:
   ```
   sum(kafka_consumergroup_lag) by (consumergroup)
   ```
3. 패널 타이틀을 "컨슈머 그룹별 Lag"으로 설정합니다
4. "Visualization" 설정에서 "Graph" 또는 "Time series" 형태를 선택합니다
5. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.7 컨슈머 그룹별 토픽 Lag 세부 정보 패널 생성

1. 대시보드에서 "Add" - "Visualization" 버튼을 클릭합니다
2. 쿼리 필드에 다음을 입력합니다:
   ```
   sum(kafka_consumergroup_lag) by (consumergroup, topic)
   ```
3. 패널 타이틀을 "컨슈머 그룹별 토픽 Lag 세부 정보"로 설정합니다
4. "Visualization" 설정에서 "Table" 형태를 선택합니다
5. "Save" 버튼을 클릭하여 패널을 저장합니다

### 7.8 대시보드 설정 및 저장

1. Save dashboard 버튼을 클릭합니다
2. 변경사항을 입력합니다.
3. "Save" 버튼을 클릭하여 대시보드를 저장합니다

### 7.9 새로고침 간격 설정

1. 대시보드 우측 상단의 refresh 버튼의 더보기를 눌러 새로고침 간격을 설정합니다(예: 10초마다)
2. 이제 대시보드가 지정된 간격으로 자동 새로고침됩니다

### 7.10 패널 배치 및 크기 조정

1. 대시보드의 각 패널을 드래그하여 원하는 위치로 이동할 수 있습니다
2. 패널의 크기는 오른쪽 아래 모서리를 드래그하여 조정할 수 있습니다
3. 만족스러운 레이아웃이 되면 Save dashboard 버튼을 클릭하여 저장합니다

---

## 8. 문제 해결

### 8.1 일반적인 문제 해결

- **컨테이너 시작 실패**: 로그 확인
  ```bash
  docker-compose logs <service-name>
  ```

- **Prometheus 타겟 접속 오류**: 네트워크 설정 확인
  ```bash
  docker-compose restart prometheus
  ```

- **Grafana 데이터 소스 오류**: 접속 URL 확인
  - Prometheus 데이터 소스 URL: `http://prometheus:9090`

### 8.2 메트릭 수집 문제

- Kafka Exporter가 메트릭을 수집하지 못하는 경우:
  ```bash
  docker-compose restart kafka-exporter
  ```

- 특정 메트릭이 표시되지 않는 경우 Prometheus UI에서 해당 메트릭 쿼리 테스트

---

## 부록: 유용한 명령어

```bash
# 토픽 목록 조회
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --list

# 컨슈머 그룹 조회
docker exec -it kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --list

# 특정 컨슈머 그룹 정보 조회
docker exec -it kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group slow-consumer
``` 
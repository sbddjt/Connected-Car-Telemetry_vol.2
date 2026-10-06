# Connected Car Telemetry vol.2

부트캠프에서 4명이 진행한 CQRS·Kubernetes 미니 프로젝트를 바탕으로, 데이터 수집과 전송의 신뢰성을 개선하는 개인 데이터 엔지니어링 프로젝트입니다.

기존 프로젝트: https://github.com/sbddjt/Connected-Car-Telemetry

## 현재 데이터 흐름

```text
SUMO (강남 도로 시뮬레이션)
  → Python / TraCI로 차량 상태 수집
  → SQLite 영속 버퍼에 저장
  → Kafka Producer 전송
  → vehicle-telemetry-v1 토픽
  → 성공 콜백에서 SQLite 완료 표시
```

SUMO와 Python은 로컬 PC에서 실행하고 Kafka 브로커 3대는 Docker Compose로 실행합니다. 토픽은 파티션 12개, 복제 계수 3, 최소 ISR 2로 구성합니다.

## 구현 범위

- 차량별 위치, 속도, 가속도, 방향, 도로·차선 정보 수집
- 실행 UUID, 차량별 순번, 이벤트 ID와 UTC 생성 시각 추가
- Kafka 전송 전 SQLite 커밋 (WAL, synchronous=FULL)
- Kafka acks=all 및 Producer 멱등성 활성화
- 성공 콜백에서 전송 완료 표시, 실패 이벤트 재시도
- 시작 시 이전 실행의 미완료 이벤트부터 전송

정상 전송과 Kafka 전체 중단·복구 실험 결과는 [실험 기록](docs/blog/part-3/part-3.md)에 정리되어 있습니다. Python 강제 종료·재시작 복구와 전체 이벤트의 누락·중복 대조 실험은 아직 수행하지 않았습니다.

## 실행 환경

- Python 3.12 이상 (현재 개발 환경: 3.13.11)
- Eclipse SUMO / sumo-gui (시나리오 생성 버전: 1.27.1)
- Docker Desktop 및 Docker Compose

SUMO를 설치하고 `sumo-gui`를 PATH에 등록합니다. 시나리오를 재생성하려면 `SUMO_HOME`도 설정해야 합니다. Python 패키지 설치만으로 SUMO 실행 파일이 설치되는 것은 아닙니다.

프로젝트 루트에서 PowerShell로 실행합니다.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

docker compose up -d
docker compose logs -f kafka-topic-init
```

`Kafka topic initialization completed.`가 출력된 후 실행합니다.

```powershell
.\.venv\Scripts\python.exe src\sumo_source.py
```

SUMO GUI가 실행되면 시뮬레이션 실행 버튼을 누릅니다. SQLite DB는 `data/event_buffer.db`에 생성됩니다. 프로그램을 종료하려면 Python 터미널에서 Ctrl+C를 누릅니다.

Kafka 수신 샘플 확인:

```powershell
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:19092 --topic vehicle-telemetry-v1 --from-beginning --max-messages 5
```

Kafka 중지 및 재시작:

```powershell
docker compose stop kafka-1 kafka-2 kafka-3
docker compose start kafka-1 kafka-2 kafka-3
```

브로커 전체 중단 시 전송 타임아웃 후 같은 이벤트를 재시도합니다. 복구될 때까지 현재 구현은 다음 SUMO 스텝도 기다립니다.

## 파일 구조

```text
src/
  sumo_source.py       # SUMO 제어, 이벤트 생성·저장
  event_buffer.py      # SQLite 저장·미완료 조회·완료 처리
  kafka_producer.py    # Kafka 전송·콜백·재시도
scenario/gangnam/      # 도로망, 차량 경로, SUMO 설정
compose.yaml          # Kafka 3개 노드와 토픽 초기화
requirements.txt      # Python 의존성
docs/blog/part-3/     # 실험 기록과 스크린샷
```

## 보장 범위와 현재 한계

보호 대상은 SQLite에 저장 완료된 이벤트입니다. 수집 직후 SQLite 커밋 전 종료되는 구간과 저장장치 손실은 보호하지 않습니다.

Kafka 저장 후 SQLite 완료 표시 전에 종료되면 재시작 시 같은 이벤트가 다시 전송될 수 있습니다. Producer 멱등성만으로 이 재전송 중복을 제거할 수 없으며, 향후 Consumer와 최종 저장소에서 event_id 기반 중복 처리가 필요합니다.

현재 수집과 전송은 같은 실행 흐름이며 이벤트를 한 건씩 전송 완료까지 기다립니다. Kafka 장애 시 수집도 멈추고, 완료 이벤트의 정리·용량 제한은 아직 없습니다. 별도 Consumer, 분석 저장소, 스트림 처리와 Kubernetes 배포는 vol.2에 아직 구현하지 않았습니다.

## 다음 단계

- 수집과 전송 분리: 통신 장애 중에도 로컬 저장 지속
- 강제 종료·재시작 복구 및 event_id 기준 누락·중복 검증
- 배치 처리와 버퍼 보존·용량 정책
- Consumer 저장 성공 이후 오프셋 커밋 및 중복 처리
- 데이터 처리·분석과 운영 지표 확장

## 지도 데이터

시나리오의 도로 데이터는 OpenStreetMap 기반입니다. © OpenStreetMap contributors. 데이터 라이선스 및 저작자 표시: https://www.openstreetmap.org/copyright

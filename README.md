# Connected Car Telemetry vol.2

SUMO로 차량을 모방하고, 차량 내부 버퍼·수신 서버·Kafka·서버 저장을 분리하는 데이터 엔지니어링 프로젝트입니다. Tesla Fleet Telemetry의 **공개 동작 정책**을 참고합니다. Python/JSON 기반 로컬 실험이며 Tesla 차량 프로토콜과 호환되는 서버는 아닙니다.

기존 프로젝트: https://github.com/sbddjt/Connected-Car-Telemetry

## 쉽게 보는 흐름

```text
SUMO → 차량 SQLite → 차량별 WebSocket → 수신 서버 → DispatchRouter
           ↑                                         ├─ Kafka: 차량 ID key → 후속 처리
           └────────── Kafka 성공 ACK 후 삭제 ────────┤  ├─ 이력 Consumer → 서버 SQLite
                                                     │  └─ 조회 Consumer → Redis 최신 상태 캐시
                                                     ├─ Redis: 차량별 Pub/Sub → SSE → 화면
                                                     └─ Logger: 콘솔에서 수신 기록 확인

화면 재접속·주기 조회 → 조회 API → Redis 최신 상태 캐시
```

서버가 받았다는 이유만으로 성공 확인을 보내지 않습니다. Kafka 최종 성공 콜백 이후에 차량에 해당 event_id의 ACK를 보냅니다. 차량은 ACK를 받은 기록만 버퍼에서 삭제합니다. ACK가 유실되면 같은 ID로 재전송하고, 서버 Consumer는 중복을 제거합니다.

## 공개 정책과 우리가 정한 정책

| 항목 | 적용 정책 | 근거 |
|---|---|---|
| 수집 묶음 | 0.5초마다 선택 신호 관찰·기록 생성 | Tesla의 500ms 수집 묶음 |
| 기록 선택 | 값이 변경되고 신호별 최소 간격이 지나면 기록, 최초 값은 기록 | Tesla의 변경 기반·최소 간격 조건 |
| 차량 버퍼 | 차량당 최대 5,000개 메시지 | Tesla 공개 버퍼 한도 |
| 재연결 | 지수 대기, 기본 최대 30초 | Tesla 공개 최대 재연결 대기 |
| 미확인 기록 | ACK가 없으면 같은 ID로 재전송 | 공개 delivery_policy=latest의 미확인 데이터 재전송 동작 |
| 성공 확인 | Kafka 최종 성공 후 차량으로 ACK | 공개 서버의 reliable_ack_sources 기능 |
| 목적지 분배 | records에서 기록 종류별 Kafka·Redis·logger 선택, ACK 기준 목적지 하나 지정 | 공개 서버의 dispatcher 구조 |
| 실시간 배포 | namespace_기록종류_{차량ID} Redis Pub/Sub 및 만료 구독자 채널 | Tesla 공개 Redis dispatcher |
| 조회 캐시·화면 | Kafka 별도 Consumer로 신호별 최신 상태를 Redis Hash에 저장, HTTP 조회 및 SSE 화면 | 우리 추가 기능; Tesla 내부 화면 저장소는 확인하지 못함 |
| 기록할 신호 | 위치, 속도; 각각 최소 1초 | 우리 선택 |
| 내부 버퍼 저장 기술 | SQLite WAL, synchronous=FULL | 우리 선택 |
| 용량 초과 | 이전 완료 행부터 정리, 부족하면 해당 차량의 오래된 미확인 기록 삭제·집계 | 우리 선택; Tesla의 상세 초과 처리 방식은 확인하지 못함 |
| 전송 순서 | 신호별 최신 후보 3번 : 오래된 이력 1번 | 우리 선택; Tesla의 상세 복구 순서는 확인하지 못함 |
| 재연결 첫 대기 | 1초부터 1→2→4→8→16→30초; Kafka ACK 성공 시 초기화 | 우리 선택; 공개된 최대 30초는 유지 |
| 동시 결과 대기 | 차량당 20개, 서버 Kafka 전체 1,000개 | 우리 선택 |
| 메시지 형식 | JSON, 차량별 WebSocket 연결 | 로컬 모방 선택; Tesla 전용 바이너리 형식과 인증 체계는 재현하지 않음 |
| 서버 저장 | 차량 버퍼와 별도 SQLite 파일에 이력·신호별 최신 상태 저장 | 우리 선택; Tesla 내부 DB 스키마는 확인하지 못함 |

공개 자료: [Tesla Fleet Telemetry 동작](https://developer.tesla.com/docs/fleet-api/fleet-telemetry), [Tesla 공개 수신 서버](https://github.com/teslamotors/fleet-telemetry).

공개 정책 상수는 `src/shared_fleet_telemetry_policy.py`, 수집할 필드와 최소 간격은 `config/vehicle_fleet_telemetry_config.json`에 있습니다. 이 정책에서는 **모든 SUMO 원본 샘플을 저장하지 않습니다.** 최소 간격 안의 중간 값과 이전 기록에서 변경되지 않은 값은 선택하지 않습니다. 선택한 메시지는 먼저 SQLite에 커밋한 뒤 전송합니다. 수집 정책에 의해 제외한 값과 버퍼 용량 초과로 삭제한 기록은 구분해야 합니다.

SUMO 시뮬레이션 시간은 0.5초씩 진행하며 이 시뮬레이션에서 관측한 신호를 묶습니다. 실제 차량 ECU의 고주파 샘플 수집과 동일한 구현은 아닙니다.

## 실행

Python 3.12 이상, SUMO/sumo-gui, Docker Desktop이 필요합니다. 현재 개발 환경은 Python 3.13.11입니다. SUMO를 설치하고 `sumo-gui`를 PATH에 등록합니다.

프로젝트 루트 PowerShell에서 의존성, Kafka와 Redis를 준비합니다.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d
docker compose logs -f kafka-topic-init
```

Windows에서 시연용 버퍼를 새로 사용해 서버·Consumer·화면·8초 SUMO를 함께 실행할 수 있습니다. 아래 구성은 기존 `data/event_buffer.db`를 소비하지 않습니다.

```powershell
.\scripts\start_local_pipeline.ps1 -QueryApiPort 8092 -VehicleBufferDbPath data/runtime/demo_vehicle_buffer.db -SumoConfigPath config/demo_sumo.sumocfg -StartSumo -SumoEndSeconds 8
```

화면은 `http://127.0.0.1:8092/`, 프로세스 PID와 로그는 `data/runtime/`입니다. SUMO는 8초에 종료되고 서버·Consumer·조회 화면은 계속 실행됩니다. 시뮬레이션 종료 후에는 마지막 관측값이 표시됩니다. `-VehicleBufferDbPath`를 생략하면 기존 차량 버퍼를 사용하며 Kafka ACK된 기록을 삭제하는 정상 정책이 적용됩니다. `-StartSumo`를 생략하면 수집기를 따로 실행할 수 있습니다. 이미 관리 중인 프로세스가 있으면 중복 실행을 거부합니다.

```powershell
# 이 스크립트가 띄운 Python 프로세스만 종료; Docker 볼륨은 유지
.\scripts\stop_local_pipeline.ps1
```

`Kafka topic initialization completed.` 확인 후 다음 명령들을 **각각 다른 터미널**에서 실행합니다.

```powershell
# 1. 수신 서버: 차량 기록을 받아 Kafka에 저장하고 성공 확인
.\.venv\Scripts\python.exe src\server_fleet_telemetry.py

# 2. 차량 전송 작업: SQLite → 수신 서버, 확인된 기록만 삭제
.\.venv\Scripts\python.exe src\vehicle_fleet_telemetry_client.py

# 3. 서버 저장 작업: Kafka → 운행 이력·최신 상태
.\.venv\Scripts\python.exe src\server_telemetry_consumer.py

# 4. 화면 조회용 최신 상태 갱신: 별도 Kafka 그룹 → Redis 캐시
.\.venv\Scripts\python.exe src\server_redis_projection_consumer.py

# 5. 조회 API·실시간 화면: 브라우저에서 http://127.0.0.1:8080
.\.venv\Scripts\python.exe src\server_vehicle_query_api.py

# 6. 차량 데이터 생성·선택·버퍼 저장
.\.venv\Scripts\python.exe src\vehicle_sumo_collector.py
```

SUMO GUI에서 실행 버튼을 누릅니다. 실행 순서는 자유입니다. 수신 서버나 Kafka가 없어도 SUMO 수집·SQLite 저장은 계속됩니다. 전송과 저장 작업은 별도 종료하며, 같은 차량 버퍼에는 전송 작업을 하나만 실행합니다.

`datastore/kafka/kafka.py`는 수신 서버가 사용하는 Producer 구현입니다. 이전 `server_kafka_dispatcher.py`는 import 호환용입니다. 이전의 직접 실행 명령 대신 `server_fleet_telemetry.py`와 `vehicle_fleet_telemetry_client.py`를 사용합니다.

### 파일과 역할

```text
src/
  vehicle_sumo_collector.py           # 차량: SUMO 관찰·신호 선택·SQLite 커밋
  vehicle_sqlite_buffer.py            # 차량: 용량 한도·미확인 기록·성공 기록 삭제
  vehicle_fleet_telemetry_client.py    # 차량: WebSocket 전송·ACK·재연결
  server_fleet_telemetry.py           # 서버: 수신·검증·연결/오류 기록·차량 ACK
  server_dispatcher.py               # 서버: 기록 종류별 목적지 분배·ACK 기준·선택 작업 한도
  server_redis_projection_consumer.py # 조회: Kafka 별도 그룹·Redis 장애 재시도
  server_redis_latest_store.py        # 조회: 신호별 최신 캐시 원자적 갱신
  server_vehicle_query_api.py         # 조회: Redis 읽기 API·Pub/Sub SSE·화면 제공
  shared_redis_config.py              # 공통: Redis 접속·namespace·캐시 이름 설정
  server_telemetry_consumer.py        # 서버: DB 저장 후 Kafka 오프셋 커밋
  server_telemetry_store.py           # 서버: 이력·신호별 최신 상태의 원자적 저장
  shared_fleet_telemetry_policy.py    # 공통: 공개 정책 상수·신호 검증·지수 대기
  shared_runtime_config.py           # 공통: 역할별 환경변수·설정 로딩
  telemetry/
    record.py                        # Tesla Record: vin·tx_type·txid·data
    producer.py                      # Tesla Dispatcher·Producer·build_topic_name
  datastore/
    kafka/kafka.py                   # Producer: Kafka 적재·최종 결과 콜백
    redis/redis.py                   # Producer: 차량별 Pub/Sub·만료 구독 채널
    simple/logger.py                 # Producer: 수신 기록 콘솔 출력
frontend/                            # 로컬 차량 목록·검색·위치/속도·실시간 갱신 화면
config/
  vehicle_fleet_telemetry_config.json # 차량: fields·interval_seconds·delivery_policy
  server_fleet_telemetry_config.json  # 서버: records·ACK 기준·목적지·토픽·접속 설정
.env.example                         # 프로젝트 환경변수 이름·기본값 참고
data/
  event_buffer.db        # 차량 내부 버퍼 모방 (여러 차량을 한 파일에 구분 저장)
  server_telemetry.db    # 서버 이력 및 최신 상태 (별도 파일)
```

두 DB는 실행 시 자동 생성합니다. 실제 차량마다 별도의 버퍼가 있는 것을 모방해, 실험에서는 한 SQLite 파일의 차량 ID별로 5,000개 한도를 적용합니다. 따라서 전체 실험 DB가 5,000개로 제한되는 것은 아닙니다.

### Tesla 공개 이름과 역할별 설정

파일명은 `vehicle_`(차량), `server_`(서버), `shared_`(공통)로 구분합니다. 클래스도 `VehicleSQLiteBuffer`, `VehicleFleetTelemetryClient`, `FleetTelemetryServer`, `KafkaDispatcher`, `ServerTelemetryStore`처럼 역할을 나타냅니다.

차량 설정은 Tesla 공개 명칭을 참고합니다.

```json
{
  "delivery_policy": "latest",
  "fields": {
    "Location": {"interval_seconds": 1},
    "VehicleSpeed": {"interval_seconds": 1}
  }
}
```

이 설정을 SUMO 수집기가 실제로 읽습니다. `Location`은 로컬 `location`, `VehicleSpeed`는 로컬 `speed_mps`에 연결합니다. 전송 JSON의 속도 단위는 m/s를 사용합니다. `latest`는 ACK 없는 기록 전체를 재전송하는 정책 이름이며, 과거 기록을 모두 버리는 뜻이 아닙니다. 현재 두 필드와 `interval_seconds`, `delivery_policy=latest`를 지원합니다. 지원하지 않는 필드·옵션은 시작 시 오류로 알려줍니다. [공개 차량 동작·설정](https://developer.tesla.com/docs/fleet-api/fleet-telemetry)

서버 설정의 `reliable_ack_sources: {"V": "kafka"}`와 `kafka.bootstrap.servers`는 공개 수신 서버 명칭을 따릅니다. 현재 Kafka 성공 후 차량 ACK 정책만 지원합니다. 변경된 ACK 정책을 조용히 무시하지 않고 시작 시 오류로 알려줍니다. [공개 서버 설정 예제](https://github.com/teslamotors/fleet-telemetry/blob/main/test/integration/config.json)

`REDIS_PASSWORD`는 Tesla 공개 서버와 같은 환경변수입니다. `VEHICLE_`와 `SERVER_` 환경변수는 **우리 프로젝트에서 정한 이름**이며, Tesla 공개 서버에 없는 설정의 역할을 구분합니다. Tesla 인증용 API 키·OAuth 설정은 구현하지 않았습니다. 차량이 연결할 WebSocket 주소와 서버의 Kafka 접속 주소를 구분합니다.

| 환경변수 | 적용 프로세스 | 의미 |
|---|---|---|
| `VEHICLE_FLEET_TELEMETRY_CONFIG_PATH` | 수집기·차량 전송 | 차량 공개 정책 설정 파일 |
| `VEHICLE_SQLITE_BUFFER_DB_PATH` | 수집기·차량 전송 | 차량 내부 SQLite 버퍼 |
| `VEHICLE_BUFFER_MAX_MESSAGES` | 수집기·차량 전송 | 차량별 버퍼 한도 |
| `VEHICLE_FLEET_TELEMETRY_SERVER_URL` | 차량 전송 | 차량이 연결할 ws/wss 수신 서버 |
| `SERVER_FLEET_TELEMETRY_CONFIG_PATH` | 수신 서버·Consumer | 서버 설정 파일 |
| `SERVER_FLEET_TELEMETRY_HOST`, `SERVER_FLEET_TELEMETRY_PORT` | 수신 서버 | 서버가 연결을 받을 주소·포트 |
| `SERVER_KAFKA_BOOTSTRAP_SERVERS`, `SERVER_KAFKA_VEHICLE_TOPIC` | 수신 서버·Consumer | Kafka 접속 주소·차량 토픽 |
| `SERVER_SQLITE_STORAGE_DB_PATH` | Consumer | 서버 이력·최신 상태 저장 DB |

전체 환경변수·기본값·인증서 경로 이름은 [.env.example](.env.example)에 있습니다. **CLI 옵션 > 프로세스 환경변수 > JSON 설정/기본값** 순서로 적용합니다. 설정은 각 프로세스 시작 시 읽으므로 변경 후 해당 프로세스를 다시 실행합니다. 상대 경로는 실행 터미널 위치와 관계없이 프로젝트 루트를 기준으로 해석합니다. 기존 SQLite 파일명은 그대로 읽어 이미 쌓인 미확인 기록을 이어서 처리합니다.

`.env.example`은 참고 파일이며 `.env`를 자동으로 읽지 않습니다. PowerShell에서 실행할 프로세스의 환경변수를 설정합니다.

```powershell
# 차량 전송 터미널
$env:VEHICLE_FLEET_TELEMETRY_SERVER_URL = "ws://127.0.0.1:8765"
.\.venv\Scripts\python.exe src\vehicle_fleet_telemetry_client.py

# 서버 터미널: Consumer 터미널에서도 같은 Kafka 주소·토픽을 사용
$env:SERVER_KAFKA_BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092,localhost:49092"
.\.venv\Scripts\python.exe src\server_fleet_telemetry.py
```

수집기와 차량 전송기는 같은 `VEHICLE_SQLITE_BUFFER_DB_PATH`를 사용해야 합니다. 수신 서버 포트를 바꾸면 차량의 `VEHICLE_FLEET_TELEMETRY_SERVER_URL`도 맞춥니다. 각 실행 파일의 `--help`에서 역할별 옵션과 대응 환경변수 이름을 볼 수 있습니다. 이전 CLI 옵션은 별칭으로 사용할 수 있습니다.

### Tesla 공개 레포와 맞춘 이름

| 공개 Go 코드 | Python 구현 | 의미 |
|---|---|---|
| `telemetry.Dispatcher` | `telemetry.producer.Dispatcher` | kafka/redis/logger 목적지 이름 |
| `telemetry.Producer.Produce(entry)` | `Producer.produce(entry)` | 수신 Record 전송 |
| `telemetry.Record.Vin` | `Record.vin` | 실험의 SUMO 차량 ID |
| `Record.TxType`, `Record.Txid` | `Record.tx_type`, `Record.txid` | 기록 종류와 전송 식별자 |
| `telemetry.BuildTopicName` | `build_topic_name` | namespace_기록종류 이름 생성 |
| `datastore/kafka/kafka.go` | `src/datastore/kafka/kafka.py` | Kafka Producer |
| `datastore/redis/redis.go` | `src/datastore/redis/redis.py` | Redis Producer |
| `datastore/simple/logger.go` | `src/datastore/simple/logger.py` | Logger Producer |
| `--config`, `REDIS_PASSWORD` | 동일한 CLI·환경변수 이름 | 서버 설정 파일·Redis 인증 |

이름은 Python 관례에 맞게 소문자와 밑줄로 표기합니다. JSON의 `records`, `reliable_ack_sources`, `namespace`, `redis.addrs/db/publish_vin_topics/subscriber_set_prefix/publish_timeout`, `logger.verbose`, `kafka.bootstrap.servers`는 공개 이름을 사용합니다. `redis.publish_timeout`은 Go `time.Duration`과 같이 **나노초 정수**이며 `5000000000`은 5초입니다. Python 내부에서 초로 변환합니다. 접속 소켓 제한과 publish 전체 제한은 별도로 적용합니다. [Tesla 설정](https://github.com/teslamotors/fleet-telemetry/blob/main/config/config.go), [Producer 인터페이스](https://github.com/teslamotors/fleet-telemetry/blob/main/telemetry/producer.go)

서버 실행 예: `python src/server_fleet_telemetry.py --config config/server_fleet_telemetry_config.json`. 이전 `--server-telemetry-config`와 `SERVER_REDIS_PASSWORD`도 호환되며, 비어 있지 않은 `REDIS_PASSWORD`가 우선합니다. 기존 `KafkaDispatcher`·`RedisDispatcher`·`LoggerDispatcher` import는 새 Producer를 참조합니다.

서버 envelope의 `vin`, `tx_type`, `txid`는 기존 JSON의 `vehicle_id`, 기록 종류, `event_id`에 연결합니다. 이미 저장된 SQLite와 로컬 전송 JSON을 읽도록 기존 컬럼·필드명은 유지합니다. Tesla 바이너리 포맷이나 실제 VIN으로 바꾼다는 뜻은 아닙니다. SQLite 버퍼·조회 캐시는 Tesla 공개 구현에 대응하는 이름이 없어 기존 역할별 이름을 유지합니다. 기존 Kafka 토픽도 유지하며 `kafka_topics`는 로컬 호환 설정입니다.

### 데이터 예시

```json
{
  "run_id": "실행 UUID",
  "event_id": "실행 UUID:차량 ID:순번",
  "vehicle_id": "car-1",
  "sequence_no": 3,
  "event_time": "2026-10-06T00:00:03.000Z",
  "simulation_time": 3.0,
  "signals": {
    "location": {"latitude": 37.5, "longitude": 127.0},
    "speed_mps": 10.0
  }
}
```

위치는 위도·경도, 속도는 m/s입니다. 변경·간격 조건을 만족한 신호만 `signals`에 들어갑니다. 속도만 바뀌면 위치가 생략될 수 있으며, 서버는 생략된 신호를 지우거나 관측 시간을 새로 갱신하지 않습니다. 이전 버전의 위도·경도·속도가 최상위 필드에 있는 기록도 전송과 서버 저장에서 해석합니다.

수신 서버는 `received_at`을 붙여 Kafka에 저장합니다. `event_time`은 관측 시각, `received_at`은 서버 수신 시각입니다.

## Python Dispatcher

Tesla 공개 서버는 `records`에서 기록 종류별 전송 대상을 정하고, `reliable_ack_sources`에서 차량 ACK 기준 목적지를 지정합니다. [공개 설정 코드](https://github.com/teslamotors/fleet-telemetry/blob/main/config/config.go)

우리의 `DispatchRouter`는 `RecordDispatcher.dispatch(record_type, record)` 인터페이스를 가진 목적지들에 기록을 보냅니다. 목적지 이름은 Tesla의 `Dispatcher` 값인 `kafka`, `redis`, `logger`이고, 구현은 각 `datastore` 패키지의 `Producer`입니다. Python import에서는 각각 `KafkaProducer`, `RedisProducer`, `LoggerProducer`로 구분합니다. 기본 서버 설정은 다음과 같습니다.

```json
{
  "records": {
    "V": ["kafka", "logger", "redis"],
    "connectivity": ["kafka", "logger", "redis"],
    "errors": ["kafka", "logger", "redis"]
  },
  "reliable_ack_sources": {"V": "kafka"},
  "kafka_topics": {
    "V": "vehicle-telemetry-v1",
    "connectivity": "vehicle-connectivity-v1",
    "errors": "vehicle-errors-v1"
  },
  "logger": {"verbose": false},
  "namespace": "telemetry_v2",
  "redis": {
    "addrs": ["127.0.0.1:6380"], "db": 0,
    "publish_vin_topics": true, "subscriber_set_prefix": "",
    "publish_timeout": 5000000000
  }
}
```

| 기록 종류 | 생성 시점·목적 | Kafka 토픽 |
|---|---|---|
| `V` | 차량 위치·속도 기록 수신; 이력·최신 상태 저장 | `vehicle-telemetry-v1` |
| `connectivity` | hello 성공 후 연결, 연결 종료; 차량 온라인 상태 관찰 | `vehicle-connectivity-v1` |
| `errors` | 서버 데이터 검증 실패·접수 한도 초과·Kafka 전송 실패 관찰 | `vehicle-errors-v1` |

위치·속도는 하나의 차량 기록 `V`에 함께 들어갑니다. 서로 다른 서비스가 위치와 속도를 각각 활용하려면 Kafka 차량 토픽을 각자 다른 Consumer 그룹으로 읽어 필요한 신호를 선택합니다. 서버 dispatcher는 여기서 기록 종류와 전송 목적지를 결정합니다.

`V`를 Kafka·Redis·logger에 함께 보내도 **Kafka 최종 성공만 차량 ACK 기준**으로 삼습니다. logger가 성공해도 Kafka 실패 시 ACK를 보내지 않으며 차량 기록을 삭제하지 않습니다. Redis·logger가 느리거나 실패해도 Kafka 성공 ACK를 지연시키지 않습니다. 재전송하면 logger에도 같은 차량 기록이 다시 출력될 수 있습니다.

`connectivity`·`errors`는 서버가 생성한 관측 기록입니다. 각 기록에 event_id·vehicle_id·event_time·connection_id를 붙이고, 오류에는 stage·source_event_id를 추가합니다. 차량 오류·연결 기록의 Tesla 전용 메시지 형식까지 재현한 것은 아닙니다. 기존 차량 Consumer는 `V` 토픽만 읽으므로 연결·오류 기록을 차량 DB 스키마에 섞지 않습니다. 연결·오류 토픽의 저장 Consumer나 화면은 아직 구현하지 않았습니다.

우리의 선택 목적지 작업은 메모리에서 최대 100개로 제한합니다. 한도 초과 접수 제외와 실패를 `optional_dropped`·`optional_failures`에 기록하고 서버 종료 시 집계를 출력합니다. 선택 목적지 작업에는 영속 버퍼·자동 재시도가 없으며, 서버 종료·과부하·목적지 장애 시 연결/오류 관측 기록이나 logger 출력이 유실될 수 있습니다. 차량 `V`는 Kafka ACK 전까지 기존 차량 SQLite 버퍼로 보호합니다. 이 한도와 관측 기록 정책은 우리 선택입니다.

`records.V`를 `["kafka"]`로 바꾸면 차량 기록 콘솔 출력을 끌 수 있습니다. logger의 `verbose=true`는 전체 기록, 기본 false는 ID·신호 이름 등의 요약을 출력합니다. `V`에서 Kafka를 빼거나 logger를 ACK 기준으로 지정하면 시작 시 오류가 발생합니다. 현재 지원하는 기록 종류는 V/connectivity/errors, 목적지는 kafka/redis/logger입니다.

`kafka_topics`는 **우리 프로젝트 설정**이며 Tesla 서버의 기본 namespace 기반 토픽 생성과 구분합니다. 기존 차량 토픽 이름을 이어 쓰면서 관측 기록을 별도 토픽으로 보냅니다. `SERVER_KAFKA_CONNECTIVITY_TOPIC`·`SERVER_KAFKA_ERRORS_TOPIC` 환경변수 또는 `--kafka-connectivity-topic`·`--kafka-errors-topic` 옵션으로 변경할 수 있습니다. 세 토픽은 서로 다른 이름을 사용합니다.

Compose 초기화 작업은 기본 세 토픽을 생성합니다. 이미 Kafka가 실행 중이면 새 연결·오류 토픽을 준비합니다.

```powershell
docker compose run --rm kafka-topic-init
```

사용자 지정 토픽으로 바꾸면 해당 이름으로 토픽도 생성해야 합니다. 설정을 수정한 뒤 수신 서버를 다시 실행합니다. 별도 dispatcher 프로세스는 필요하지 않으며 수신 서버 내부에서 비동기로 동작합니다.

## Redis의 역할: 공개 Dispatcher와 별도 조회 기능

| 구성 | 역할 | 구현 파일 |
|---|---|---|
| Kafka Dispatcher | topic으로 전송, 차량 ID를 key로 사용, 성공 확인 후 차량 ACK | `datastore/kafka/kafka.py` |
| Redis Dispatcher | 차량별 channel로 즉시 publish, 관제·화면에 수신 기록 전달 | `datastore/redis/redis.py` |
| Logger Dispatcher | 수신 기록을 stdout으로 요약 또는 전체 JSON 출력 | `datastore/simple/logger.py` |

SUMO 차량 ID가 실험의 VIN 역할을 합니다. Logger는 현재 콘솔 출력이며, 로그 파일 목적지는 아직 추가하지 않았습니다.

Tesla 공개 Redis 코드는 **Pub/Sub 배포**를 구현합니다. 이 공개 코드만으로 Tesla 화면이 Redis 최신 상태 DB를 읽는다고 판단할 수 없습니다. 우리 Dispatcher도 같은 목적을 따릅니다. [Tesla Redis dispatcher](https://github.com/teslamotors/fleet-telemetry/blob/main/datastore/redis/redis.go)

기본 채널은 `telemetry_v2_V_{car-1}`, `telemetry_v2_connectivity_{car-1}`, `telemetry_v2_errors_{car-1}`입니다. 기록 종류별 namespace 채널이며, payload는 수신 기록 JSON입니다. 서버 설정 `redis.publish_vin_topics=true`가 차량 채널을 켭니다. `subscriber_set_prefix`를 설정하면 `접두사_telemetry_v2_V_{car-1}` sorted set의 멤버를 추가 목적지 채널로 사용합니다. 점수는 Unix 초 단위 만료 시각이고, 현재 초보다 작은 멤버를 제거합니다. `publish_vin_topics=false`라면 구독자 접두사가 필수입니다. 화면에서 특정 차량을 조회하면 별도 구독 채널에 30초 lease를 등록하고 10초마다 갱신합니다. 전체 차량 스트림은 차량별 기본 채널을 사용하므로 `publish_vin_topics=true`가 필요합니다.

Redis Pub/Sub는 재전송·이력 저장 기능이 없습니다. 구독자가 없거나 연결이 끊긴 동안의 publish는 나중에 재생되지 않습니다. 따라서 차량 기록 보존 기준은 Kafka로 유지합니다. [Redis Pub/Sub 보장](https://redis.io/docs/latest/develop/pubsub/)

화면의 재접속 조회를 위해 **우리 추가 기능**으로 `server_redis_projection_consumer.py`가 Kafka V 토픽을 독립 그룹 `telemetry-redis-latest-v1`로 읽고 Redis 최신 상태를 만듭니다. 기존 이력 저장 Consumer와 서로 다른 그룹이라 각자 모든 차량 기록을 처리합니다. 위치·속도는 관측 시각을 각각 비교하며, Lua 스크립트로 비교·갱신을 한 번에 실행합니다. 늦게 온 과거 기록은 이력에 남고 화면 최신 값을 되돌리지 않습니다. 같은 시각이면 같은 run_id의 더 큰 순번만 반영합니다.

조회 Consumer는 Redis 저장 **후** Kafka 오프셋을 커밋합니다. Redis 장애 시 파티션 소비를 일시 정지하고 1→2→4→…→30초 대기하며 재시도합니다. 이때 Kafka poll을 유지해 재할당을 처리하고, 소유권을 잃은 파티션은 새 소유자가 미커밋 위치부터 처리합니다. 이력 Consumer·수신 서버·차량 수집기는 계속 동작합니다.

화면은 Redis Pub/Sub를 SSE로 전달받아 즉시 갱신하고, 기본 1초마다 Redis 캐시도 조회합니다. 캐시와 실시간 기록이 다른 순서로 도착해도 화면 역시 신호별 관측 시각을 비교합니다. 선택할 수 있는 정보는 위치·속도이고, API의 m/s 속도를 화면에서는 km/h로 표시합니다. 최근 관측 시간은 차량의 연결 상태를 뜻하지 않습니다. Pub/Sub 수신 시점은 Kafka 저장 성공보다 빠를 수 있습니다.

- 목록: `GET /api/vehicles?signals=location,speed_mps&limit=50&offset=0`
- 차량: `GET /api/vehicles/car-1?signals=location`
- 실시간: `GET /api/stream?vehicle_id=car-1` (생략하면 전체 차량 V 채널)
- 준비 상태: `GET /api/health` (Redis 장애는 503)

기본 접속은 `SERVER_REDIS_URL=redis://127.0.0.1:6380/0`입니다. Pub/Sub namespace는 `SERVER_REDIS_NAMESPACE`, 조회 키 접두사는 `SERVER_REDIS_CACHE_PREFIX`로 구분합니다. 수신 서버·조회 Consumer·API는 같은 Redis 접속 설정을 사용합니다. Compose의 Redis는 호스트 6380 포트와 AOF 볼륨을 사용합니다. Pub/Sub는 AOF에 저장되는 데이터가 아닙니다. 캐시는 `telemetry:v2:{query}:vehicle:차량ID`, 목록은 `telemetry:v2:{query}:vehicles`에 저장하고 현재 TTL은 적용하지 않습니다.

Redis 캐시가 삭제되면 기존 Consumer 그룹의 커밋된 기록은 자동으로 다시 읽히지 않습니다. Kafka 보관 기간 안의 기록으로 재구축하려면 조회 Consumer를 중지하고 **새 그룹 이름**으로 실행합니다. 새 그룹은 earliest부터 처리합니다.

```powershell
.\.venv\Scripts\python.exe src\server_redis_projection_consumer.py --kafka-redis-consumer-group telemetry-redis-rebuild-20261006
```

Redis 주소는 하나의 standalone 서버만 지원합니다. Tesla 공개 구현의 Cluster/Sentinel 등 전체 접속 옵션·프로토콜을 그대로 복제한 것은 아닙니다. 공개 서버는 시작 시 Redis ping을 요구하지만, 우리는 Redis 장애가 Kafka 적재를 막지 않도록 최초 publish 때 접속합니다. Redis Dispatcher 실패·과부하의 실시간 출력은 재시도하지 않으며, 최신 상태 캐시는 Kafka Consumer로 복구합니다. 서버의 logger/Redis 선택 작업 한도는 공유하므로 logger가 오래 막히면 실시간 publish가 생략될 수 있습니다. 실험 정책으로 명시합니다.

## 단절·재전송

- 차량이 수신 서버에 접속하지 못하면 버퍼에 남겨 최대 30초 간격으로 재연결합니다.
- Kafka 실패·큐 접수 실패 시 서버는 성공 ACK를 보내지 않습니다. 차량은 연결을 다시 만들며 보존 기록을 재전송합니다.
- 기본 ACK 대기 제한은 45초, Kafka 전송 시도의 기본 제한은 30초입니다.
- 같은 차량에서 전송 중이거나 ACK 후 로컬 삭제를 기다리는 기록은 다시 접수하지 않습니다. SQLite 삭제 실패는 프로세스가 살아 있는 동안 삭제만 재시도합니다.
- 수신 서버에서 같은 ID·내용이 Kafka 결과 대기 중이면 하나의 Kafka 전송 결과에 함께 연결합니다. 서버 재시작이나 ACK 유실 이후의 재전송은 Kafka에 중복으로 들어갈 수 있습니다.
- 5초 동안 전송할 기록과 미확인 기록이 없는 차량의 연결은 정리하며 새 기록이 생기면 다시 연결합니다.
- 이미 접수된 메시지는 취소하거나 우선순위를 바꾸지 않습니다. 복구 직후 기존 전송 중 기록이 먼저 도착할 수 있습니다. 새 접수 후보를 고를 때 신호별 최신 기록을 우선합니다.

차량 전송 옵션 예시:

```powershell
.\.venv\Scripts\python.exe src\vehicle_fleet_telemetry_client.py --vehicle-reconnect-initial-seconds 1 --vehicle-reconnect-max-seconds 30 --vehicle-max-in-flight 20 --vehicle-live-weight 3 --vehicle-ack-timeout-seconds 45
```

로컬 기본 연결은 `ws://127.0.0.1:8765`입니다. 자체 인증서로 mTLS 실험을 할 경우 서버는 `--server-tls-cert --server-tls-key --server-tls-client-ca`, 차량 전송은 `--fleet-telemetry-server-url wss://... --vehicle-tls-ca-file --vehicle-tls-client-cert --vehicle-tls-client-key`을 제공합니다. 인증서의 차량 ID 매핑과 Tesla 인증 프로토콜은 구현하지 않았습니다.

## 이력과 최신 상태

Consumer는 하나의 DB 트랜잭션에서 다음을 처리하고, 완료 후 해당 Kafka 메시지의 소비 오프셋을 동기로 커밋합니다.

1. `telemetry_history`: event_id당 한 번만 저장합니다. 늦게 도착한 과거 기록도 저장합니다. 같은 ID에 다른 내용이 있으면 오류로 중단합니다.
2. `vehicle_latest_state`: 차량·신호별 한 행을 유지합니다. 더 새로운 event_time일 때만 갱신합니다. 시각이 같으면 같은 run_id 내의 더 큰 sequence_no일 때만 갱신합니다.

예를 들어 10:05 위치 C 이후 10:01 위치 A가 도착하면 이력에는 둘 다 남고 최신 위치는 C입니다. 위치는 10:05에, 속도는 10:03에 관측될 수 있으므로 신호별 시간을 각각 비교합니다. event_time은 UTC로 정규화한 뒤 비교하며, 서로 다른 실행의 순번만 비교하지 않습니다.

Kafka 커밋 실패 후 같은 기록을 다시 소비해도 중복 저장하지 않습니다. 데이터 검증·DB 저장 실패 시 해당 오프셋을 넘기지 않고 Consumer가 중단합니다. Kafka 일시 오류는 재연결을 기다립니다. 차량 목록 화면은 Redis로 조회하며, 지도 위에 위치를 표시하는 기능은 아직 구현하지 않았습니다.

## 버퍼 정책과 보장 범위

신규 기록 저장·용량 초과 정리·집계를 한 트랜잭션으로 커밋합니다. `buffer_stats.capacity_dropped`에는 용량 때문에 삭제한 미확인 기록 수를 누적합니다. 정책에 따라 삭제된 기록은 차량 버퍼에서 복구하지 못합니다. 기존 DB는 보존하며 다음 신규 저장부터 해당 차량에 한도가 적용됩니다. 이전 버전의 전송 완료 행은 차량 전송 작업 시작 시 정리합니다.

한도는 차량별 메시지 수이며 디스크 바이트 용량이나 보관 기간 제한은 아닙니다. 차량 수가 많으면 실험 전체 DB도 커집니다. SQLite의 쓰기 잠금·저장장치 장애와 커밋 전 종료 구간은 수집에 영향을 줄 수 있습니다.

차량에서 삭제 가능한 시점은 Kafka 성공 확인입니다. 최종 서버 DB 저장까지 확인하는 ACK는 아니므로 Kafka 보관 정책이 Consumer 처리 지연을 충분히 감당하도록 운영해야 합니다. 서버 DB는 실험용 SQLite이며 대규모 운영용 저장소를 검증한 것은 아닙니다.

## 검증

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node tests\test_vehicle_state.js
```

임시 SQLite와 실제 로컬 WebSocket을 사용해 변경·간격 기반 수집, 차량별 한도, Kafka ACK 전 보관, 단절·복구, ACK 유실과 중복, 신호별 최신 후보, 과거 이력의 최신 상태 되돌림 방지, DB 트랜잭션 및 오프셋 커밋 순서, dispatcher의 목적지 분배·Kafka 기준 ACK·느린 logger·선택 목적지 실패·작업 한도를 검증합니다. Kafka는 모의 Producer를 사용하며, 별도 테스트에서 실제 confluent-kafka 클라이언트의 브로커 연결 실패·타임아웃도 확인합니다. Redis는 fakeredis와 Lua 실행기로 Pub/Sub·lease 만료·원자적 최신 갱신·동시 쓰기·장애 복구·오프셋 커밋·실제 로컬 HTTP/SSE를 검증합니다. Node 테스트는 화면 최신 상태 비교를 검증합니다. 실제 Kafka 3개 브로커·Redis·SUMO headless를 연결한 장애·복구 검증을 통과했습니다. [2026-10-06 검증 결과](docs/verification-2026-10-06.md)에 환경·결과·재현 명령을 기록했습니다. SUMO GUI로 수행한 장시간 실험은 별도입니다.

기존 직접 전송 구조의 실험 기록: [part-3](docs/blog/part-3/part-3.md). 새 수신 서버 구조의 실험 결과와는 구분합니다.

## 지도 데이터

시나리오 도로 데이터는 OpenStreetMap 기반입니다. © OpenStreetMap contributors. 라이선스 및 저작자 표시: https://www.openstreetmap.org/copyright

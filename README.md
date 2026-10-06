# Connected Car Telemetry vol.2

SUMO로 차량을 모방하고, 차량 내부 버퍼·수신 서버·Kafka·서버 저장을 분리하는 데이터 엔지니어링 프로젝트입니다. Tesla Fleet Telemetry의 **공개 동작 정책**을 참고합니다. Python/JSON 기반 로컬 실험이며 Tesla 차량 프로토콜과 호환되는 서버는 아닙니다.

기존 프로젝트: https://github.com/sbddjt/Connected-Car-Telemetry

## 쉽게 보는 흐름

```text
SUMO → 차량 SQLite → 차량별 WebSocket → 수신 서버 → Kafka Producer → Kafka
           ↑                                                        ├─ 이력 Consumer → 서버 SQLite
           └──────────── Kafka 성공 ACK 후 해당 기록 삭제 ────────────┘  └─ 조회 Consumer → Redis 최신 상태
                                                                                           ↓
                                                                                       조회 API
                                                                                           ↓
                                                                               강남 차량 관제 지도 (1초 조회)
```

수신 서버는 Kafka 한 곳으로만 보냅니다. 별도 Dispatcher와 Redis/Logger Producer는 제거했습니다. 두 Consumer는 서로 다른 그룹으로 동일한 차량 토픽을 읽습니다. 이력 DB는 수신 기록을 보관하고, Redis는 화면 조회에 필요한 차량별 최신 위치·속도를 보관합니다. Redis 서버는 한 개이며, 화면은 조회 API를 통해 캐시를 읽습니다. Redis Pub/Sub와 SSE 경로는 사용하지 않습니다.

서버가 받았다는 이유만으로 성공 확인을 보내지 않습니다. Kafka 최종 성공 콜백 이후에 차량에 해당 event_id의 ACK를 보냅니다. 차량은 ACK를 받은 기록만 버퍼에서 삭제합니다. ACK가 유실되면 같은 ID로 재전송하고, 서버 Consumer는 중복을 제거합니다.

## 공개 정책과 우리가 정한 정책

| 항목 | 적용 정책 | 근거 |
|---|---|---|
| 수집 묶음 | 0.5초마다 선택 신호 관찰·기록 생성 | Tesla의 500ms 수집 묶음 |
| 기록 선택 | 값이 변경되고 신호별 최소 간격이 지나면 기록, 최초 값은 기록 | Tesla의 변경 기반·최소 간격 조건 |
| 차량 버퍼 | 차량당 최대 5,000개 메시지 | Tesla 공개 버퍼 한도 |
| 재연결 | 지수 대기, 기본 최대 30초 | Tesla 공개 최대 재연결 대기 |
| 미확인 기록 | ACK가 없으면 같은 ID로 재전송 | 공개 delivery_policy=latest의 미확인 데이터 재전송 동작 |
| 성공 확인 | 수신 서버가 Kafka 최종 성공 후 차량으로 ACK | 공개 서버의 reliable_ack_sources 정책 참고 |
| 조회 캐시·화면 | Kafka 별도 Consumer로 신호별 최신 상태를 Redis Hash에 저장, HTTP 주기 조회 화면 | 우리 추가 기능; Tesla 내부 화면 저장소는 확인하지 못함 |
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

Windows에서 시연용 버퍼를 사용해 서버·Consumer·지도·30분 SUMO를 함께 실행할 수 있습니다. 아래 구성은 기존 `data/event_buffer.db`를 소비하지 않습니다.

```powershell
.\scripts\start_local_pipeline.ps1 -QueryApiPort 8092 -VehicleBufferDbPath data/runtime/demo_vehicle_buffer.db -SumoConfigPath config/demo_sumo.sumocfg -StartSumo -SumoEndSeconds 1800
```

화면은 `http://127.0.0.1:8092/`, 프로세스 PID와 로그는 `data/runtime/`입니다. SUMO는 1,800초에 종료되고 서버·Consumer·조회 화면은 계속 실행됩니다. 시뮬레이션 종료 후에는 마지막 관측값이 표시됩니다. `-VehicleBufferDbPath`를 생략하면 기존 차량 버퍼를 사용하며 Kafka ACK된 기록을 삭제하는 정상 정책이 적용됩니다. `-StartSumo`를 생략하면 수집기를 따로 실행할 수 있습니다. 이미 관리 중인 프로세스가 있으면 중복 실행을 거부합니다.

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

# 5. 조회 API·차량 정보 화면: 브라우저에서 http://127.0.0.1:8080
.\.venv\Scripts\python.exe src\server_vehicle_query_api.py

# 6. 차량 데이터 생성·선택·버퍼 저장
.\.venv\Scripts\python.exe src\vehicle_sumo_collector.py
```

SUMO GUI에서 실행 버튼을 누릅니다. 실행 순서는 자유입니다. 수신 서버나 Kafka가 없어도 SUMO 수집·SQLite 저장은 계속됩니다. 전송과 저장 작업은 별도 종료하며, 같은 차량 버퍼에는 전송 작업을 하나만 실행합니다.

`datastore/kafka/kafka.py`는 수신 서버가 직접 사용하는 Kafka Producer입니다. 수신 서버는 `server_fleet_telemetry.py`, 차량 전송기는 `vehicle_fleet_telemetry_client.py`로 실행합니다.

### 파일과 역할

```text
src/
  vehicle_sumo_collector.py           # 차량: SUMO 관찰·신호 선택·SQLite 커밋
  vehicle_sqlite_buffer.py            # 차량: 용량 한도·미확인 기록·성공 기록 삭제
  vehicle_fleet_telemetry_client.py    # 차량: WebSocket 전송·ACK·재연결
  server_fleet_telemetry.py           # 서버: 수신·검증·연결/오류 기록·차량 ACK
  server_redis_projection_consumer.py # 조회: Kafka 별도 그룹·Redis 장애 재시도
  server_redis_latest_store.py        # 조회: 신호별 최신 캐시 원자적 갱신
  server_vehicle_query_api.py         # 조회: Redis 읽기 API·차량 정보 화면 제공
  shared_redis_config.py              # 공통: Redis 캐시 접속·키 접두사 설정
  server_telemetry_consumer.py        # 서버: DB 저장 후 Kafka 오프셋 커밋
  server_telemetry_store.py           # 서버: 이력·신호별 최신 상태의 원자적 저장
  shared_fleet_telemetry_policy.py    # 공통: 공개 정책 상수·신호 검증·지수 대기
  shared_runtime_config.py           # 공통: 역할별 환경변수·설정 로딩
  shared_kafka.py                    # 공통: Kafka 기록별 토픽·전송 오류
  telemetry/
    record.py                        # Tesla Record: vin·tx_type·txid·data
  datastore/
    kafka/kafka.py                   # Producer: Kafka 적재·최종 결과 콜백
frontend/                            # Leaflet 강남 지도·차량 목록·관측 정보·1초 조회 화면
config/
  vehicle_fleet_telemetry_config.json # 차량: fields·interval_seconds·delivery_policy
  server_fleet_telemetry_config.json  # 서버: Kafka ACK 기준·토픽·조회 Redis 접속 설정
.env.example                         # 프로젝트 환경변수 이름·기본값 참고
data/
  event_buffer.db        # 차량 내부 버퍼 모방 (여러 차량을 한 파일에 구분 저장)
  server_telemetry.db    # 서버 이력 및 최신 상태 (별도 파일)
```

차량 버퍼 SQLite는 임베디드 저장소 역할이며, 서버 이력 SQLite는 실험용 저장소입니다. 서버 이력은 나중에 별도 DB로 교체할 수 있고, Redis는 화면 조회용 최신 상태를 계속 담당합니다. 두 DB는 실행 시 자동 생성합니다. 실제 차량마다 별도의 버퍼가 있는 것을 모방해, 실험에서는 한 SQLite 파일의 차량 ID별로 5,000개 한도를 적용합니다. 따라서 전체 실험 DB가 5,000개로 제한되는 것은 아닙니다.

### Tesla 공개 이름과 역할별 설정

파일명은 `vehicle_`(차량), `server_`(서버), `shared_`(공통)로 구분합니다. 클래스도 `VehicleSQLiteBuffer`, `VehicleFleetTelemetryClient`, `FleetTelemetryServer`, `KafkaProducer`, `ServerTelemetryStore`처럼 역할을 나타냅니다.

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
| `telemetry.Producer.Produce(entry)` | Kafka `Producer.produce(entry)` | 수신 Record를 Kafka에 전송 |
| `telemetry.Record.Vin` | `Record.vin` | 실험의 SUMO 차량 ID |
| `Record.TxType`, `Record.Txid` | `Record.tx_type`, `Record.txid` | 기록 종류와 전송 식별자 |
| `datastore/kafka/kafka.go` | `src/datastore/kafka/kafka.py` | Kafka Producer |
| `--config`, `REDIS_PASSWORD` | 동일한 CLI·환경변수 이름 | 설정 파일·조회 Redis 인증 |

공개 이름은 필요한 부분에 유지하며, Python 내부에서는 소문자와 밑줄을 사용합니다. `reliable_ack_sources`는 Kafka ACK 정책을 명시하지만 목적지 선택 기능은 없습니다. `kafka_topics`는 우리 프로젝트의 고정 토픽 이름 설정입니다. [Tesla 설정](https://github.com/teslamotors/fleet-telemetry/blob/main/config/config.go), [Producer 인터페이스](https://github.com/teslamotors/fleet-telemetry/blob/main/telemetry/producer.go)

`vin`, `tx_type`, `txid`는 기존 JSON의 `vehicle_id`, 기록 종류, `event_id`에 연결합니다. 차량 버퍼·서버 이력의 컬럼과 전송 JSON, Kafka 토픽 이름은 유지합니다. Python/JSON 실험으로 Tesla 바이너리 프로토콜·실제 차량 인증과 호환되지 않습니다.

서버 설정은 `--config` 또는 기존 `--server-telemetry-config`로 지정합니다. 조회 Consumer와 API는 비어 있지 않은 `REDIS_PASSWORD`를 우선하고 `SERVER_REDIS_PASSWORD`를 호환용으로 지원합니다. 이전 Dispatcher 모듈·import와 라우팅 설정은 제거했으므로 `records`, `logger`, `namespace`, Redis publish/구독 설정 및 관련 CLI는 더 이상 지원하지 않습니다.

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

## 수신 서버 → Kafka

`FleetTelemetryServer`가 검증한 차량 기록을 `KafkaProducer.produce(Record)`로 직접 보냅니다. Producer는 차량 ID를 Kafka key로 사용하고 `acks=all`, `enable.idempotence=true`로 적재합니다. native Producer의 큐에 들어간 것만으로 성공 처리하지 않으며, `poll()`이 최종 성공 콜백을 처리한 뒤 차량으로 ACK를 보냅니다.

기본 서버 설정은 다음과 같습니다. `redis`는 조회 Consumer와 API에서만 사용하며 수신 서버는 Redis에 접속하지 않습니다.

```json
{
  "host": "127.0.0.1",
  "port": 8765,
  "reliable_ack_sources": {"V": "kafka"},
  "kafka": {
    "bootstrap.servers": "localhost:29092,localhost:39092,localhost:49092",
    "message.timeout.ms": 30000
  },
  "kafka_topics": {
    "V": "vehicle-telemetry-v1",
    "connectivity": "vehicle-connectivity-v1",
    "errors": "vehicle-errors-v1"
  },
  "redis": {"addrs": ["127.0.0.1:6380"], "db": 0}
}
```

| 기록 종류 | 목적 | Kafka 토픽 |
|---|---|---|
| `V` | 차량 위치·속도 기록; 이력·최신 상태 저장 | `vehicle-telemetry-v1` |
| `connectivity` | 차량 연결·종료 관찰 | `vehicle-connectivity-v1` |
| `errors` | 검증 실패·접수 한도 초과·Kafka 전송 실패 관찰 | `vehicle-errors-v1` |

연결·오류 기록도 Kafka로 직접 전송합니다. 각 기록에 event_id·vehicle_id·event_time·connection_id를 붙이고, 오류에는 stage·source_event_id를 추가합니다. 두 차량 Consumer는 `V` 토픽만 읽습니다. 연결·오류 토픽의 저장 Consumer나 화면은 아직 구현하지 않았습니다.

연결·오류 기록은 수신 서버 내부의 비동기 메모리 작업으로 최대 100개를 접수합니다. 실패·한도 초과를 `notification_failures`·`notification_dropped`로 집계합니다. 영속 버퍼가 없으므로 서버 종료·Kafka 장애·과부하 때 관측 기록이 유실될 수 있습니다. 차량 `V`는 ACK 전까지 차량 SQLite에 보관하며, 연결·오류 처리 완료를 기다리지 않습니다.

Logger Producer는 제거했습니다. 연결·종료, Kafka 관측 기록 전송 실패, 종료 집계는 일반 Python logging으로 기록합니다. 실행 스크립트는 stdout/stderr를 `data/runtime/` 로그 파일로 남깁니다. 로그 출력은 Kafka 성공 또는 차량 ACK를 대신하지 않습니다.

`SERVER_KAFKA_CONNECTIVITY_TOPIC`·`SERVER_KAFKA_ERRORS_TOPIC` 또는 대응 CLI로 토픽을 변경할 수 있습니다. 세 토픽은 서로 다른 이름이어야 합니다. Compose 초기화는 기본 세 토픽을 만들며, 사용자 지정 토픽은 별도로 생성해야 합니다.

```powershell
docker compose run --rm kafka-topic-init
```

## Kafka Consumer에서 이력과 조회 기능 분리

| Consumer | 기본 Consumer Group | 저장소·역할 |
|---|---|---|
| 이력 Consumer | `telemetry-storage-v1` | 서버 SQLite에 전체 수신 기록과 신호별 최신 상태 저장 |
| 조회 Consumer | `telemetry-redis-latest-v1` | Redis에 차량별 최신 위치·속도 저장 |

같은 차량 토픽을 서로 다른 Consumer Group으로 읽으므로 각자 모든 차량 기록을 처리합니다. 조회용 Redis 캐시는 우리 추가 기능이며 Tesla 앱 내부의 조회 저장 구조를 그대로 재현한 것은 아닙니다. 읽기 모델을 별도 만드는 CQRS 방식으로 지도·차량 정보 서비스에 연결할 수 있습니다.

조회 Consumer는 위치·속도의 관측 시각을 각각 비교하고 Lua 스크립트로 비교·갱신을 원자적으로 처리합니다. 늦게 온 과거 기록은 이력에 저장하되 Redis 최신 값을 되돌리지 않습니다. 같은 시각이면 같은 run_id의 더 큰 순번만 반영합니다.

Redis 저장 **후** Kafka 오프셋을 동기로 커밋합니다. Redis 장애 시 파티션 소비를 일시 정지하고 1→2→4→…→30초 대기하며 재시도합니다. Kafka poll을 유지해 재할당을 처리하고 소유권을 잃은 파티션은 새 소유자가 미커밋 위치부터 처리합니다. 이력 Consumer·수신 서버·차량 수집기는 독립적으로 계속 동작합니다.

## Redis 조회 API와 차량 화면

브라우저는 Redis에 직접 연결하지 않고 조회 API를 사용합니다. 첫 화면과 검색에서 조회하고, 자동 갱신을 켜면 기본 1초마다 조회합니다. 목록 API를 200개씩 읽어 최대 1,000대를 화면에 적재하며, 전체 수와 표시 수를 구분합니다. Redis Pub/Sub, SSE, `/api/stream`은 제거했습니다. 따라서 화면은 Kafka에 적재된 후 조회 Consumer가 캐시에 반영한 상태를 보여줍니다.

Leaflet 지도에 SUMO에서 변환한 실제 강남 위도·경도로 차량 마커를 표시합니다. 현재 데이터는 **SUMO 시뮬레이션**이며 실제 차량 GPS 수신 기능은 아닙니다. 차량 목록·마커를 선택하면 속도(km/h), 좌표, 위치·속도의 개별 관측 시각을 확인할 수 있습니다. 검색, 최근 관측 필터, 전체 차량 보기, 강남 영역 복귀, 선택 차량 따라가기, 자동/수동 갱신을 지원합니다.

새 위치가 오면 최대 800ms 동안 마커를 보간합니다. 같은 실행에서 5초 이내·150m 이내의 연속 위치만 보간하고, 단절 후 복구·큰 위치 변화·새 실행은 최신 좌표로 즉시 이동합니다. 화면 보간은 원본 좌표와 DB 기록을 바꾸지 않습니다. 화면도 신호별 관측 시각을 비교해 늦게 온 과거 데이터로 최신 값을 되돌리지 않습니다.

선택 차량의 궤적은 **이 화면에서 관측한 최근 80개 좌표**입니다. 전체 운행 이력을 DB에서 조회한 경로는 아닙니다. 신호가 15초 이내에 관측됐는지 표시하며, 이것을 차량 통신 연결 상태로 해석하지 않습니다. 조회 API 장애 중에도 마지막 위치·정보를 유지하고 자동 갱신으로 복구를 확인합니다.

배경 지도는 OpenStreetMap 타일을 사용하며 Leaflet 1.9.4 JS/CSS는 저장소에 포함했습니다. 타일 연결이 실패하면 기존 SUMO 도로망에서 추출한 `frontend/gangnam_roads.geojson`의 304개 도로를 표시합니다. 지도 중심·초기 줌·타일 URL은 `frontend/map_config.json`에서 설정합니다. 다른 타일 서비스로 바꾸면 API의 이미지 CSP 허용 주소도 함께 수정해야 합니다. 작은 화면에서는 지도와 상세 패널을 먼저 보여주고 차량 목록을 아래에 배치합니다.

- 목록: `GET /api/vehicles?signals=location,speed_mps&limit=50&offset=0`
- 차량: `GET /api/vehicles/car-1?signals=location`
- 준비 상태: `GET /api/health` (Redis 장애는 503)

Redis 서버는 하나입니다. 기본 `SERVER_REDIS_URL=redis://127.0.0.1:6380/0`, 키 접두사는 `SERVER_REDIS_CACHE_PREFIX=telemetry:v2:{query}`입니다. 조회 Consumer와 API만 동일한 Redis 접속·캐시 설정을 사용합니다. Compose의 Redis는 호스트 6380 포트와 AOF 볼륨을 사용합니다. 차량 Hash는 `telemetry:v2:{query}:vehicle:차량ID`, 목록은 `telemetry:v2:{query}:vehicles`이며 TTL은 적용하지 않습니다. standalone Redis 한 개를 지원합니다.

Redis 캐시가 삭제되면 기존 그룹의 커밋된 기록은 자동으로 다시 읽히지 않습니다. Kafka 보관 기간 안의 기록으로 재구축하려면 조회 Consumer를 중지하고 **새 그룹 이름**으로 실행합니다. 새 그룹은 earliest부터 처리합니다.

```powershell
.\.venv\Scripts\python.exe src\server_redis_projection_consumer.py --kafka-redis-consumer-group telemetry-redis-rebuild-20261006
```

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

Kafka 커밋 실패 후 같은 기록을 다시 소비해도 중복 저장하지 않습니다. 데이터 검증·DB 저장 실패 시 해당 오프셋을 넘기지 않고 Consumer가 중단합니다. Kafka 일시 오류는 재연결을 기다립니다. 차량 지도·목록·상세 정보는 조회 API를 통해 Redis 최신 상태를 읽습니다. 서버 이력 DB 조회 API는 아직 구현하지 않았습니다.

## 버퍼 정책과 보장 범위

신규 기록 저장·용량 초과 정리·집계를 한 트랜잭션으로 커밋합니다. `buffer_stats.capacity_dropped`에는 용량 때문에 삭제한 미확인 기록 수를 누적합니다. 정책에 따라 삭제된 기록은 차량 버퍼에서 복구하지 못합니다. 기존 DB는 보존하며 다음 신규 저장부터 해당 차량에 한도가 적용됩니다. 이전 버전의 전송 완료 행은 차량 전송 작업 시작 시 정리합니다.

한도는 차량별 메시지 수이며 디스크 바이트 용량이나 보관 기간 제한은 아닙니다. 차량 수가 많으면 실험 전체 DB도 커집니다. SQLite의 쓰기 잠금·저장장치 장애와 커밋 전 종료 구간은 수집에 영향을 줄 수 있습니다.

차량에서 삭제 가능한 시점은 Kafka 성공 확인입니다. 최종 서버 DB 저장까지 확인하는 ACK는 아니므로 Kafka 보관 정책이 Consumer 처리 지연을 충분히 감당하도록 운영해야 합니다. 서버 DB는 실험용 SQLite이며 대규모 운영용 저장소를 검증한 것은 아닙니다.

## 검증

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node tests\test_vehicle_state.js
node tests\test_map_motion.js
```

임시 SQLite와 로컬 WebSocket으로 Kafka 최종 성공 전 버퍼 보관, ACK 유실·재전송, 신호별 최신 후보, 단절·복구, 직접 Kafka 전송 및 연결·오류 작업 한도를 검증합니다. 이력 DB·Redis는 저장 후 오프셋 커밋, 중복·과거 기록 처리, 원자적 갱신·장애 복구를 검증합니다. 조회 API의 캐시 조회·필터·오류 응답과 Pub/Sub 없이 조회되는 동작도 확인합니다. Node 테스트는 화면 최신 상태 비교와 위치 보간·단절 후 즉시 이동 조건을 검증합니다.

실제 SUMO·SQLite·WebSocket·Kafka 3개 브로커·Redis 검증 스크립트는 임시 DB·별도 토픽·캐시를 사용합니다. `--fault-injection`은 이 vol.2 Compose의 Redis·Kafka를 실제 중단·재시작하며, 종료 시 복구합니다.

```powershell
.\.venv\Scripts\python.exe scripts\verify_live_pipeline.py --fault-injection --report data/live-verification-kafka-only.json
```

지도 화면의 실제 데이터·브라우저 검증은 [강남 관제 지도 검증](docs/verification-2026-10-06-map.md)에 기록합니다. 현재 Kafka 단일 수신 구조의 결과는 [Dispatcher 제거 후 검증](docs/verification-2026-10-06-kafka-only.md)에 기록합니다. [이전 Dispatcher 구조 검증](docs/verification-2026-10-06.md)은 당시 구조의 실험 기록입니다.

기존 직접 전송 구조의 실험 기록: [part-3](docs/blog/part-3/part-3.md). 새 수신 서버 구조의 실험 결과와는 구분합니다.

## 지도 데이터

시나리오 도로 데이터와 배경 타일은 OpenStreetMap 기반입니다. © OpenStreetMap contributors. [저작자 표시·라이선스](https://www.openstreetmap.org/copyright), [타일 사용 정책](https://operations.osmfoundation.org/policies/tiles/). 화면 우측 하단에 저작자 표시를 유지합니다. 타일을 일괄 다운로드하거나 오프라인 캐시를 만들지 않습니다. 로컬 도로망은 이미 보유한 SUMO 파일에서 생성합니다.

Leaflet 1.9.4는 BSD 2-Clause 라이선스이며 `frontend/vendor/leaflet/LICENSE`에 원문을 포함합니다. [Leaflet 공식 배포](https://leafletjs.com/download.html).

도로망이 바뀌었을 때 다음 명령으로 지도용 도로를 다시 생성합니다. SUMO와 traci가 필요하며 차량 버퍼·Kafka 데이터는 변경하지 않습니다.

```powershell
.\.venv\Scripts\python.exe scripts\build_gangnam_roads.py
```

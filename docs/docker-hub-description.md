# Connected Car Telemetry v2

**한국어** · SUMO 차량 시뮬레이션부터 데이터 전송, 이력 저장, 최신 상태 조회와 지도 화면까지 연결하는 컨테이너 기반 텔레메트리 프로젝트입니다.

**English** · A containerized vehicle telemetry project connecting SUMO simulation, buffered delivery, historical storage, latest-state queries, and a map dashboard.

[GitHub source](https://github.com/sbddjt/Connected-Car-Telemetry_vol.2)

## 한국어

### 프로젝트 소개

Connected Car Telemetry v2는 기존 차량 텔레메트리 프로젝트를 발전시킨 데이터 엔지니어링 실험입니다. 차량 데이터 생성·전송·수신·저장·조회를 분리하고 Docker 이미지와 Kubernetes 워크로드로 실행하도록 구성했습니다.

[Tesla Fleet Telemetry의 공개 동작](https://developer.tesla.com/docs/fleet-api/fleet-telemetry)과 [공개 수신 서버](https://github.com/teslamotors/fleet-telemetry)를 참고하여 변경 기반 수집, 최소 기록 간격, 미확인 데이터 보관·재전송, Kafka 저장 확인 이후의 ACK를 적용합니다. Python과 JSON으로 구현한 자체 시뮬레이션이며, Tesla 공식 제품 또는 실제 Tesla 차량 프로토콜과 호환되는 서버는 아닙니다.

### 전체 서비스 흐름

```text
SUMO 차량 시뮬레이션
  → 수집기 → SQLite 영속 버퍼 → WebSocket 전송기
  → 수신 서버 → Kafka
                   ├→ 이력 Consumer → MongoDB
                   └→ 최신 상태 Consumer → Redis
                                             ↓
                                      조회 API → 지도 화면
```

수신 서버는 Kafka 적재 성공을 확인한 뒤 차량 전송기에 ACK를 보냅니다. 전송기는 해당 이벤트의 ACK를 확인한 뒤 SQLite 기록을 삭제합니다. 이 ACK는 MongoDB의 최종 저장 완료와는 구분됩니다.

### 이미지와 실행 역할

| 태그 | 용도 |
| --- | --- |
| `v0.1.0` | 전송기, 수신 서버, Consumer, 조회 API에 공통으로 사용하는 서버 이미지 |
| `v0.1.0-simulator` | SUMO 1.27.1과 강남 확장 시나리오를 포함한 수집기 이미지 |

서버 이미지는 실행 인자로 역할을 선택합니다. 같은 이미지에서 역할별 컨테이너를 각각 실행합니다.

| 실행 인자 | 역할 |
| --- | --- |
| `vehicle-sender` | SQLite 미확인 이벤트를 WebSocket으로 전송 |
| `receiver` | 차량 연결 수신, Kafka 적재 및 ACK |
| `history-consumer` | MongoDB에 이벤트 이력 저장 |
| `redis-consumer` | Redis에 신호별 최신 차량 상태 반영 |
| `query-api` | 조회 API와 지도 화면 제공 |
| `vehicle-collector` | SUMO 관측 데이터 수집; 시뮬레이터 이미지 사용 |

Kafka·MongoDB·Redis는 이 이미지에 내장되지 않으며 각각 공식 이미지로 실행합니다. 현재 게시된 두 태그의 실행 플랫폼은 **Linux/AMD64**입니다. 다른 CPU 구조에서는 별도 빌드가 필요합니다.

### 이미지 받기

```sh
docker pull sbddjt/connected-car-telemetry-v2:v0.1.0
docker pull sbddjt/connected-car-telemetry-v2:v0.1.0-simulator

# 역할의 옵션 확인: 서비스 전체를 시작하는 명령은 아닙니다.
docker run --rm --memory=128m sbddjt/connected-car-telemetry-v2:v0.1.0 receiver --help
docker run --rm --memory=256m sbddjt/connected-car-telemetry-v2:v0.1.0-simulator vehicle-collector --help
```

이미지만 받으면 전체 서비스가 자동 연결되지는 않습니다. Kafka·MongoDB·Redis 주소, 컨테이너 간 네트워크와 SQLite·DB의 영속 저장 공간을 함께 구성해야 합니다.

### 노트북용 Kubernetes 구성

프로젝트 소스의 `scripts/deploy_kubernetes.ps1`은 `laptop` 프로필을 기본으로 사용합니다.

- Kafka 1개, 복제 계수 1, 최소 ISR 1, 최대 JVM heap 256MiB.
- MongoDB·Redis 각각 1개, 서버 역할별 인스턴스 1개.
- SUMO 동시 차량 상한 5대, 첫 실행은 시뮬레이션 시간 60초.
- 수집기와 전송기는 같은 Pod에서 하나의 SQLite PVC를 공유합니다.
- 시뮬레이터는 기본적으로 정지 상태이며 명시적으로 시작합니다.
- 구성 요소를 순차 배포하고, Windows 여유 RAM과 다음 단계의 메모리 예산을 확인합니다.
- 초기 조회는 외부 공개 대신 `port-forward`를 사용합니다.

프로젝트 소스를 보유하고 Docker Desktop의 Kubernetes와 기본 동적 StorageClass가 준비된 Windows 환경의 예시입니다.

```powershell
.\scripts\deploy_kubernetes.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Context docker-desktop -Profile laptop

# 메모리 여유를 확인한 뒤 시뮬레이션까지 시작
.\scripts\deploy_kubernetes.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Context docker-desktop -Profile laptop -StartSimulation

kubectl --context docker-desktop -n telemetry-v2 port-forward service/query-api 8092:8080
```

서비스 준비 후 지도 주소는 `http://127.0.0.1:8092/`입니다. 첫 운행 완료 후 수집기는 대기하고 전송기는 남은 이벤트를 계속 전송합니다. 다시 배포하면 시뮬레이터는 기본 정지 상태로 돌아가므로 실행 의도를 확인해야 합니다.

Kafka 3개를 사용하는 `replicated` 프로필도 준비되어 있으나 자원이 충분한 환경에서 별도로 실험합니다. 프로필을 바꾸며 기존 Kafka 데이터 볼륨을 자동 축소하지 않습니다. 메모리 제한은 노트북 전체의 안전한 동작을 보장하는 수치가 아니므로 실제 사용량을 함께 확인해야 합니다.

### 데이터 보존 설계와 범위

- **먼저 저장하고 전송:** 선택된 텔레메트리를 SQLite WAL 버퍼에 커밋한 뒤 전송합니다.
- **ACK 전 보관:** Kafka 성공 확인 전에는 해당 버퍼 기록을 삭제하지 않습니다. 연결 단절이나 ACK 유실 시 재전송합니다.
- **버퍼 포화 시 대기:** Kubernetes 실험은 `VEHICLE_BUFFER_OVERFLOW_POLICY=block`을 사용합니다. 미확인 기록을 버리는 대신 신규 묶음을 롤백하고 같은 이벤트를 재시도하며 SUMO 진행을 멈춥니다. 기존 로컬 실행의 기본 정책은 `drop_oldest`이므로 실행 설정을 확인해야 합니다.
- **중복 처리:** MongoDB는 `event_id`를 문서 ID로 사용하며 저널 확인 이후 Kafka 오프셋을 커밋합니다. Redis는 늦거나 중복된 관측이 최신 상태를 되돌리지 않도록 처리합니다.
- **컨테이너와 데이터 분리:** Kafka·MongoDB·Redis·SQLite에 PVC를 사용하고 StatefulSet 삭제·축소 시 PVC를 유지합니다.

목표는 **SQLite에 커밋한 선택 이벤트를 재전송하고 이력을 중복 없이 저장하는 것**입니다. 모든 원본 샘플을 기록하거나 모든 장애에서 무손실을 보장한다는 의미는 아닙니다. 단일 Kafka 브로커, 단일 MongoDB, PVC 삭제, 저장장치 손실, PC 전체 장애와 SQLite 커밋 전 강제 종료에는 별도 대응이 필요합니다. Kafka 자동 보관 만료를 비활성화하므로 디스크 사용량과 Consumer lag를 관리해야 합니다.

### 확인된 상태 · 2026-10-08

- 서버·시뮬레이터 이미지 빌드와 Docker Hub 업로드 완료.
- Python 테스트 109개 통과.
- 시뮬레이터 이미지에서 차량 상한 5대·시뮬레이션 3초 실행 확인: 메모리 제한 256MiB, CPU 제한 0.5.
- 최소 Kubernetes 구성의 서버 측 사전 검증 통과.
- Kafka 1개의 Kubernetes 정상 기동을 확인했습니다. 다음 서비스 추가는 노트북 메모리 보호 조건에 걸려 중단했으며, 현재 Kafka와 Docker Desktop을 정지하고 PVC를 유지했습니다. Kubernetes 전체 흐름·장애 복구·지속 처리량 검증은 아직 완료하지 않았습니다.

## English

### About the project

Connected Car Telemetry v2 is a data engineering experiment that extends an existing vehicle telemetry project. Collection, delivery, reception, persistence, and queries run as separate Docker containers managed through Kubernetes manifests.

The design follows publicly documented ideas from [Tesla Fleet Telemetry](https://developer.tesla.com/docs/fleet-api/fleet-telemetry) and its [open-source receiver](https://github.com/teslamotors/fleet-telemetry): change-based collection, minimum recording intervals, buffered unacknowledged events, retries, and acknowledgment after successful Kafka delivery. This is an independent Python/JSON simulation, not an official Tesla product or a server compatible with the Tesla vehicle protocol.

### End-to-end flow

```text
SUMO simulation
  → Collector → Persistent SQLite buffer → WebSocket sender
  → Receiver → Kafka
                 ├→ History consumer → MongoDB
                 └→ Latest-state consumer → Redis
                                              ↓
                                      Query API → Map dashboard
```

The receiver acknowledges an event after Kafka reports successful delivery. The sender then removes that event from SQLite. This acknowledgment does not mean that MongoDB has already persisted the event.

### Images and roles

| Tag | Purpose |
| --- | --- |
| `v0.1.0` | Shared server image for the sender, receiver, consumers, and query API |
| `v0.1.0-simulator` | Collector image including SUMO 1.27.1 and the expanded Gangnam scenario |

Select a role through the container argument; each role runs in a separate container.

| Argument | Role |
| --- | --- |
| `vehicle-sender` | Deliver unacknowledged SQLite events over WebSocket |
| `receiver` | Receive events, publish to Kafka, and return ACKs |
| `history-consumer` | Persist event history in MongoDB |
| `redis-consumer` | Maintain the latest state for each vehicle signal in Redis |
| `query-api` | Serve the query API and map dashboard |
| `vehicle-collector` | Collect SUMO observations using the simulator image |

Kafka, MongoDB, and Redis run from their own official images. The two published tags currently target **Linux/AMD64**; other architectures require a separate build.

### Pull and inspect

```sh
docker pull sbddjt/connected-car-telemetry-v2:v0.1.0
docker pull sbddjt/connected-car-telemetry-v2:v0.1.0-simulator

# Inspect options; these commands do not start the full pipeline.
docker run --rm --memory=128m sbddjt/connected-car-telemetry-v2:v0.1.0 receiver --help
docker run --rm --memory=256m sbddjt/connected-car-telemetry-v2:v0.1.0-simulator vehicle-collector --help
```

Pulling an image does not configure the complete system. The pipeline also requires service endpoints, container networking, and persistent volumes for SQLite and the databases.

### Minimal Kubernetes profile

The deployment script in the project source defaults to the `laptop` profile:

- One Kafka broker: replication factor 1, minimum ISR 1, maximum JVM heap 256MiB.
- One MongoDB, one Redis, and one instance of each server role.
- A maximum of five concurrent simulated vehicles for an initial 60 seconds of simulation time.
- Collector and sender in the same Pod, sharing a single SQLite PVC.
- Simulator stopped by default and started explicitly.
- Sequential deployment with checks for available Windows RAM and the next stage's memory budget.
- Local dashboard access through port forwarding.

With the project source, Docker Desktop Kubernetes, and a default dynamic StorageClass available on Windows:

```powershell
.\scripts\deploy_kubernetes.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Context docker-desktop -Profile laptop

# Start simulation after checking memory headroom
.\scripts\deploy_kubernetes.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Context docker-desktop -Profile laptop -StartSimulation

kubectl --context docker-desktop -n telemetry-v2 port-forward service/query-api 8092:8080
```

Once services are ready, open `http://127.0.0.1:8092/`. After the finite scenario ends, the collector waits while the sender continues draining buffered events. Reapplying the manifests restores the simulator's default stopped state unless explicitly started.

A separate `replicated` profile provides three Kafka brokers for experiments on a machine with sufficient resources. Existing broker volumes are not automatically downsized when switching profiles. Container limits do not guarantee laptop-wide memory safety; monitor actual usage as well.

### Delivery and durability boundaries

- Commit selected telemetry to SQLite WAL before attempting delivery.
- Keep buffered events until Kafka delivery is acknowledged; retry after disconnects or lost ACKs.
- Use `VEHICLE_BUFFER_OVERFLOW_POLICY=block` for Kubernetes experiments. At capacity, roll back the new batch, retry the same events, and pause SUMO instead of dropping pending events. Legacy local runs still default to `drop_oldest`, so verify the selected policy.
- Use `event_id` as the MongoDB document ID and commit Kafka offsets only after journal-acknowledged storage. Redis prevents older or duplicate observations from replacing newer state.
- Use PVCs for Kafka, MongoDB, Redis, and SQLite, retaining claims when StatefulSets are deleted or scaled down.

The goal is to replay **selected events already committed to SQLite** and store their history without duplicates. This is not a guarantee that every raw sample or every failure scenario is lossless. A single Kafka broker, a single MongoDB instance, deleted PVCs, storage loss, whole-machine failure, and termination before SQLite commit require additional protection. Kafka automatic retention expiry is disabled, so disk usage and consumer lag must be monitored.

### Verified status · 2026-10-08

- Both images built and uploaded to Docker Hub.
- All 109 Python tests passed.
- Simulator image executed a five-vehicle-cap, three-second simulation with a 256MiB memory limit and 0.5 CPU limit.
- Minimal manifests passed Kubernetes server-side dry-run validation.
- One Kafka broker started successfully on Kubernetes. The memory guard stopped deployment before adding the next service. Kafka and Docker Desktop are now stopped, with the PVC retained. Full Kubernetes delivery, failure recovery, and sustained-throughput verification remain pending.

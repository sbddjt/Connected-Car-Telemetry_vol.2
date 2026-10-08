# Docker Hub → Kubernetes 운영 준비

vol.2를 `sbddjt/connected-car-telemetry-v2`에 올리고 Kubernetes에서 실행하는 초기 구성입니다.
서버 이미지 `v0.1.0`는 receiver/history-consumer/redis-consumer/query-api/vehicle-sender 역할을 공유합니다.
`v0.1.0-simulator`에는 headless SUMO와 강남 확장 시나리오가 추가됩니다.
`.dockerignore`는 소스·필요 설정·지도·압축 시나리오만 허용합니다. `.env`, SQLite, 로그, 인증서와 Git 이력은 이미지에 포함하지 않습니다.

## 노트북용 실행 규모

8GB 노트북용 `laptop` 프로필을 추가했습니다. 배포 스크립트의 기본값입니다.
Kafka 1개(RF=1/minISR=1, heap 256MiB), MongoDB·Redis 각 1개, Consumer 역할별 1개, SUMO 차량 상한 5대·60초입니다.
서버/DB 장애 시 ACK·SQLite 보관·재전송·중복 제거는 유지하지만 Kafka 단일 브로커의 디스크 손실을 복제로 복구하지 못합니다.
`replicated` 프로필은 Kafka 3개(RF=3/minISR=2), 차량 상한 20대를 유지합니다. 자원이 충분한 환경의 복제/장애 실험용입니다.
프로필 사이에서 기존 Kafka PVC의 quorum과 복제 설정을 자동 변경하지 않습니다. 다른 환경으로 전환할 때는 별도 클러스터/namespace와 데이터 이동 계획이 필요합니다.
Kubernetes 자체 시스템 Pod도 메모리를 쓰며, 노트북 안의 노드 2개가 실제 RAM 2배를 제공하지는 않습니다.

## 배치 구조

```text
vehicle-simulator StatefulSet (기본 replicas=0, 실행 시 1)
  ├─ SUMO collector → SQLite WAL / FULL (PVC)
  └─ vehicle sender ← 같은 SQLite 파일
           │ 차량별 WebSocket
           ▼
receiver Deployment → Kafka StatefulSet (laptop: 1개 / replicated: 3개, 각 PVC)
           │             ├─ history-consumer Deployment → MongoDB StatefulSet (PVC)
    Kafka 성공 ACK        └─ redis-consumer Deployment → Redis StatefulSet (PVC)
           │                                              ↓
   해당 ID만 SQLite 삭제                         query-api Deployment + 지도
```

MongoDB·Redis는 단일 노드입니다. 복제 프로필의 Kafka 3개도 한 PC의 Docker Desktop에서는 같은 장애 영역에 놓입니다.
서버 프로그램에는 자동 마운트된 서비스 계정 토큰을 사용하지 않고, 일반 사용자로 실행합니다.
외부 공개 포트는 만들지 않습니다. 초기 화면 확인은 `port-forward`를 사용합니다.
실제 차량 장치의 버퍼는 차량 측에 있어야 합니다. 클러스터 안의 SUMO+SQLite는 시험용 차량 모방입니다.

## 빌드와 업로드

Docker Desktop의 Linux 엔진을 실행하고, 프로젝트 루트에서 로컬로 로그인합니다. 비밀번호·토큰은 채팅이나 파일에 기록하지 않습니다.

```powershell
docker login -u sbddjt
.\scripts\build_push_images.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Push
```

스크립트는 두 이미지를 빌드하고 각 역할의 `--help`, SUMO 실행 파일을 확인한 뒤 업로드합니다.
다음 변경에는 새 버전 태그를 사용합니다. 공개 저장소는 클러스터가 바로 받을 수 있습니다.
비공개 저장소는 Docker Hub 인증용 imagePullSecret을 별도로 만들고 telemetry-v2의 default ServiceAccount에 연결해야 합니다.
클러스터 노드와 같은 CPU 구조에서 빌드하는 기본 구성입니다. ARM/AMD64 혼합 클러스터에는 별도 multi-platform 빌드가 필요합니다.

## Kubernetes 배포

클러스터가 실행 중이고, Linux 노드에 동적 PVC를 제공하는 기본 StorageClass가 있어야 합니다.
`kubectl get storageclass`로 기본 클래스가 있는지 먼저 확인합니다. 없으면 PVC가 Pending에 머뭅니다.
다른 클러스터에서는 `-Context`를 해당 컨텍스트 이름으로 바꿉니다.

```powershell
kubectl config get-contexts
kubectl --context docker-desktop get nodes
kubectl --context docker-desktop get storageclass
.\scripts\deploy_kubernetes.ps1 -Repository sbddjt/connected-car-telemetry-v2 -Version v0.1.0 -Context docker-desktop -Profile laptop -StartSimulation
kubectl --context docker-desktop -n telemetry-v2 port-forward service/query-api 8092:8080
```

화면: `http://127.0.0.1:8092/`. 노트북 프로필 첫 실행은 동시 차량 상한 5대, 시뮬레이션 60초입니다.
완료한 collector는 대기하고 sender는 미전송 기록을 계속 배출합니다. Pod 재시작 때는 새 운행 run_id로 시나리오를 다시 시작하며 기존 SQLite 기록은 유지합니다.
시뮬레이션 과정의 메모리 상태는 복원하지 않습니다. collector/sender를 각각 여러 Pod로 늘리지 않습니다.
두 컨테이너가 같은 노드의 로컬 파일시스템 PVC를 공유해야 하며 SQLite WAL에 NFS를 사용하지 않습니다.

배포는 Kafka·MongoDB·Redis의 준비, 토픽 초기화 완료, 서버 배포를 순서대로 기다립니다.
`.build/kubernetes-laptop` 또는 `.build/kubernetes-replicated`에 실제 이미지 이름으로 렌더링합니다. `deploy/kubernetes-laptop`은 노트북용, `deploy/kubernetes`는 복제 구성 검토용 기본 파일이며 생성기는 `scripts/render_kubernetes.py`입니다.
다시 적용하면 simulator의 replicas가 기본 0으로 돌아갑니다. 실행을 유지하려면 `-StartSimulation`을 사용합니다.
성공한 kafka-topics Job은 재배포 때 다시 실행되지 않습니다. 토픽 정책을 변경할 때는 Job 재실행 계획이 별도로 필요합니다.

## 데이터 보존 조건

목표는 **SQLite에 커밋한 선택 텔레메트리를 장애 뒤 다시 보내고 MongoDB에 중복 없이 저장하는 것**입니다.
모든 SUMO 원본 샘플을 기록하는 것은 아닙니다. Tesla의 변경 기반·최소 간격 정책을 참고하여 선택한 데이터만 기록합니다.

- 차량: ACK 이전에 SQLite에서 삭제하지 않습니다. Kubernetes collector는 `VEHICLE_BUFFER_OVERFLOW_POLICY=block`으로 실행하여 한도가 차면 신규 묶음을 롤백하고 같은 ID·내용으로 저장을 재시도합니다. 그동안 SUMO 단계 진행을 멈춥니다. 기존 로컬 스크립트의 기본값은 이전과 같은 `drop_oldest`입니다.
- block 정책은 단일 SQLite 파일에 적용합니다. 샤드 간 전역 원자성이 없으므로 collector에서 block+sharded 설정은 오류로 거부합니다.
- 수신: Kafka 최종 성공 콜백 후 ACK합니다. Producer는 `acks=all`, idempotence를 사용합니다. ACK 유실 시 같은 ID로 재전송합니다.
- Kafka: 복제 프로필은 RF=3/minISR=2, 노트북 프로필은 RF=1/minISR=1이며, unclean leader election 비활성입니다. 차량 토픽은 compact하지 않고 자동 시간·용량 삭제를 비활성화했습니다. **디스크가 무한히 늘지는 않습니다.** 여유 용량·Consumer lag를 관리하고 이력이 저장됐는지 확인한 후 보관/정리 정책을 정해야 합니다.
- 이력: MongoDB `_id=event_id`, 저널 확인 이후 파티션 오프셋을 커밋합니다. 저장/커밋 실패 시 재처리합니다. Redis는 별도 그룹으로 신호별 최신 상태를 만듭니다.
- Pod: SIGTERM을 기존 프로그램의 종료 경로에 연결하고 60초의 종료 유예를 둡니다. Kafka·MongoDB·Redis·SQLite는 emptyDir가 아닌 PVC입니다. StatefulSet 삭제/축소 때 PVC를 유지합니다.

이 구성은 Pod 재시작·수신 단절·DB 일시 장애 복구를 실험하는 구성입니다. PVC 삭제, 저장장치 손실, 동일 PC 전체의 디스크/전원 장애까지 무손실을 증명하지 않습니다.
Kafka ACK는 MongoDB 최종 저장 ACK가 아니고 디스크 fsync 완료를 직접 보증하는 ACK도 아닙니다. 실제 운영에는 독립 장애 영역의 Kafka 복제, MongoDB replica set, 백업/복원 및 장애 실험이 추가로 필요합니다.
SQLite 커밋 전 메모리에만 있던 수집 묶음은 강제 종료 시 잃을 수 있습니다. 기록 생성 시점까지 보장 범위를 넓히려면 수집 체크포인트/원본 재생 설계가 필요합니다.
연결/오류 알림 토픽은 제한된 메모리 작업이며 실패·한도 초과 시 알림 유실이 가능하므로 차량 텔레메트리의 보존 범위와 구분합니다.

## 운영과 검증

```powershell
kubectl --context docker-desktop -n telemetry-v2 get pods,pvc
kubectl --context docker-desktop -n telemetry-v2 logs deployment/receiver --tail=100
kubectl --context docker-desktop -n telemetry-v2 logs deployment/history-consumer --tail=100
kubectl --context docker-desktop -n telemetry-v2 logs vehicle-simulator-0 -c sender --tail=100
kubectl --context docker-desktop -n telemetry-v2 exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-0.kafka:9092 --describe --group telemetry-history-mongodb-v1
kubectl --context docker-desktop -n telemetry-v2 scale deployment/history-consumer --replicas=2
```

Consumer는 역할별 같은 그룹을 사용하여 최대 12개 파티션을 분담합니다. 먼저 1개에서 시작합니다.
기존 `/api/pipeline`은 같은 파일시스템의 JSON만 읽으므로 Kubernetes 서버 Pod 전체 지표를 집계하지 못합니다. 이 화면 값을 클러스터 처리량으로 해석하지 않습니다.
클러스터 모니터링은 현재 kubectl 상태·로그·Kafka consumer lag 확인까지입니다. Prometheus/exporter 통합은 별도 작업입니다.
query-api readiness는 Redis 상태를 확인하지만 liveness는 정적 화면을 확인하므로 Redis 장애 때문에 API를 반복 재시작하지 않습니다.
receiver readiness는 소켓 개방만 확인하며 Kafka 적재 가능 여부는 실제 ACK 결과로 확인합니다.

무손실 판정은 실행 전에 기대한 event_id를 확보하여 장애 복구 후 MongoDB ID 집합과 비교해야 합니다. 처리 건수만 같거나 지도에 차량이 보이는 것으로 무손실을 판정하지 않습니다.
실제 배포 후 receiver 재시작, Kafka 브로커 1개 재시작, MongoDB 일시 중지, ACK 유실, 버퍼 포화 실험을 수행하고 미확인 ID가 최종 저장되는지 확인합니다.
실험 중 PVC나 namespace를 삭제하지 않습니다. 시뮬레이터를 멈추기 전에 sender의 pending_events=0을 확인합니다.
진행 중인 수집 단계의 강제 종료는 위 커밋 전 보장 한계에 포함됩니다.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node tests\test_vehicle_state.js
node tests\test_map_motion.js
kubectl kustomize deploy/kubernetes
```

이 파일은 실행 계획이며 실제 이미지 빌드·push·Kubernetes 장애 실험 성공을 의미하지 않습니다. 실제 확인 결과는 [2026-10-08 검증 기록](verification-2026-10-08-kubernetes.md)에 남깁니다.
현재 8GB 노트북에서 Kafka 3개, DB, Docker와 Kubernetes를 동시에 운영하는 것은 자원 부담이 큽니다. 클러스터 노드 자원과 여유 RAM을 확인하고 소규모로 시작합니다.

## 근거

- [Tesla Fleet Telemetry 공개 동작](https://developer.tesla.com/docs/fleet-api/fleet-telemetry): 변경/최소 간격, 버퍼, 재연결, 미확인 데이터 재전송.
- [Tesla 공개 서버](https://github.com/teslamotors/fleet-telemetry): Kafka reliable ACK와 Kubernetes/Helm 운영.
- [Kubernetes StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/), [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
- [Kafka topic 설정](https://kafka.apache.org/41/configuration/topic-configs/): retention 및 min.insync.replicas.

## 메모리 제한과 단계적 확대

배포 스크립트는 시작과 각 단계 전에 Windows 여유 RAM을 확인합니다. 기본 최소 여유 512MB에 다음 단계의 컨테이너 메모리 한도 합계를 더한 예산이 부족하면 확대를 중단하고 기존 데이터와 작업은 유지합니다. laptop 프로필에서 Kafka·MongoDB·토픽 초기화·서버 묶음·시뮬레이터 추가 전에는 각각 1,024MB 이상, Redis 추가 전에는 640MB 이상을 요구합니다. 이미 실행 중인 단계의 재적용에도 같은 보수적 기준을 적용합니다. 이 검사는 WSL 캐시 회수와 갑작스러운 시스템 부하를 완전히 예측하거나 방지하는 보장은 아닙니다.
Pod에는 CPU/메모리 제한을 두고 처음에는 laptop 프로필만 실행합니다. 빌드와 운행 부하 시험을 동시에 수행하지 않습니다.
`inspect_kubernetes_resources.ps1 -Context docker-desktop`는 별도 metrics-server 없이 kubelet 통계에서 프로젝트 컨테이너의 실제 메모리·CPU를 확인합니다. 권한이 없으면 통계 조회가 실패합니다. Windows/Docker/시스템 Pod 메모리는 컨테이너 합계에 포함되지 않습니다.
기능 확인 뒤 5→10→20대로 한 단계씩 늘리고, 미전송 버퍼·처리 지연·Consumer lag·호스트 여유 RAM을 함께 봅니다. 병목을 확인한 뒤 Consumer를 하나씩 추가합니다. Kafka 3개 복제 실험은 별도 자원 여유가 있을 때 진행합니다.
## 작은 데이터 흐름 검증

서비스가 모두 준비되면 query-api Pod에서 아래 검증을 실행합니다. 표시된 테스트 차량에 이벤트 2개와 중복 1회를 보내고 Kafka ACK 3회, 임시 SQLite 비움, MongoDB 고유 ID 2개와 Redis 최신 속도를 비교합니다. MongoDB·Redis에는 표시된 시험 기록을 남깁니다. 이 검증은 전체 SUMO 기록의 보존이나 장애 복구 시험을 대신하지 않습니다.

```powershell
Get-Content scripts/verify_kubernetes_delivery.py -Raw |
  kubectl --context docker-desktop -n telemetry-v2 exec -i deployment/query-api -- python -
```

Docker Hub 상세 설명은 [한국어·영어 게시용 문서](docker-hub-description.md)를 사용합니다.

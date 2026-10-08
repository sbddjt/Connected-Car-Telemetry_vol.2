# 2026-10-08 Docker Hub·Kubernetes 준비 검증

## 현재 결과

vol.2의 서버와 SUMO를 이미지로 만들고 `sbddjt/connected-car-telemetry-v2`에 업로드했습니다. 최소 Kubernetes 배포 구성과 메모리 보호 스크립트를 추가했습니다. 실제 클러스터에는 `telemetry-v2` namespace, ConfigMap, Kafka StatefulSet과 PVC를 적용하고 **Kafka 1개의 정상 기동을 확인했습니다.** 다음 MongoDB 단계는 메모리 보호 조건에 걸려 중단했습니다. 이후 Kafka를 replicas 0으로 축소하고 Docker Desktop을 정상 종료했습니다. Kafka PVC와 데이터, 이미지, 배포 파일은 유지했습니다. 전체 서비스 흐름 검증은 보류 상태입니다.

## 게시한 이미지

현재 두 이미지 모두 Linux/AMD64입니다. attestation 항목은 다른 CPU용 실행 이미지가 아닙니다.

| 태그 | 원격 이미지 인덱스 digest |
| --- | --- |
| `sbddjt/connected-car-telemetry-v2:v0.1.0` | `sha256:dd964c30b935bfe466626e7f8cea5449f5d51c7384914298fb0134e6875fbbc8` |
| `sbddjt/connected-car-telemetry-v2:v0.1.0-simulator` | `sha256:a2754a941a03c57dc78a0a37f2126e997c81d631bec74b0fff8e5fbc602d7f43` |

`docker push`와 `docker buildx imagetools inspect`로 두 태그의 게시를 확인했습니다. 인증 정보는 기록하지 않았습니다.

## 통과한 확인

- 최종 Python 테스트: `python -m unittest discover -s tests -v`, **109개 / 42.229초 / OK**.
- JavaScript 차량 상태 및 지도 움직임 테스트 2종 통과.
- 이미지의 서버 역할 5종과 수집기 역할 `--help` 확인.
- 서버 이미지 빌드 성공. 시뮬레이터 이미지에서 SUMO 1.27.1 실행 확인.
- 실제 시뮬레이터 컨테이너를 메모리 256MiB, CPU 0.5, 차량 상한 5대·시뮬레이션 3초로 실행: 종료 코드 0, SQLite 선택 이벤트 13개 생성. 이 기록은 임시 파일시스템에 저장한 단독 수집기 시험이며 Kafka 연동 시험은 아닙니다.
- 두 배포 프로필의 `kubectl kustomize` 렌더링 확인.
- `kubectl --context docker-desktop apply --dry-run=server -k deploy/kubernetes-laptop`: 전체 리소스의 API 사전 검증 통과. dry-run은 실제 Pod를 실행하지 않습니다.
- 메모리 보호 조건이 실제 부족 상태에서 추가 서비스 배포를 차단했습니다.

버퍼 포화 테스트는 신규 묶음 롤백·기존 미확인 기록 유지·재시작 후 보관·ACK 후 동일 묶음 재시도 등을 확인합니다. Kafka/MongoDB 실패·ACK 유실 테스트도 포함하지만 이 테스트 결과를 실제 Kubernetes 장애 복구 검증으로 해석하지 않습니다.

## 자원 관찰과 중단 이유

- Windows 물리 RAM 약 8GB.
- Docker 엔진 29.5.2, VM 메모리 약 3.8GiB.
- Docker Desktop Kubernetes v1.34.3의 노드 2개가 Ready. 노드 수가 실제 노트북 RAM을 늘리지는 않습니다.
- 빌드 후 파일 캐시를 정리하여 여유 메모리를 확인했습니다. 데이터, 볼륨과 다른 앱을 삭제하거나 종료하지 않았습니다.
- 최초 배포 시작 시 Windows 여유 RAM 574MB, namespace·설정 적용 후 441MB여서 당시 최소 여유 512MB 조건에 걸려 중단.
- 이후 여유 RAM은 약 213~621MB 사이에서 변동했습니다. Kubernetes 사전 검증 시 약 293MB, 문서 저장·지도 테스트 이후 약 213MB였습니다. 시점별 관찰이며 지속 부하 측정이 아닙니다.
- 최종 보호 조건은 최소 여유 512MB에 다음 단계 컨테이너 메모리 한도 합계를 더합니다. laptop 프로필의 Kafka 추가 전에는 최소 1,024MB를 요구하므로 현재 상태에서 재배포해도 서비스 확대를 막습니다.
- VS Code·Chrome 등 다른 앱도 메모리를 사용 중이지만 사용자의 작업을 종료하지 않았습니다. 무거운 모니터링 서비스나 별도 metrics-server를 설치하지 않았습니다.

## 구성 변경

- 서버 역할을 선택하는 공통 이미지와 별도 SUMO 이미지.
- Kafka·MongoDB·Redis 각 1개, 서버 역할별 1개, SUMO 상한 5대·60초인 laptop 프로필.
- 별도 replicated 프로필은 Kafka 3개, RF 3/minISR 2. 노트북에서는 실행하지 않았습니다.
- SQLite 공유 PVC와 `block` 포화 정책. 기본 시뮬레이터 replicas 0.
- 단계별 배포, 실제 컨테이너 사용량 조회와 작은 데이터 흐름 검증 스크립트.
- 운영 문서 및 Docker Hub 한국어·영어 설명.

## 남은 확인

노트북 여유 메모리를 확보한 후 [운영 가이드](kubernetes.md)의 laptop 배포를 재개합니다. Kafka → MongoDB → Redis → 토픽 초기화 → 서버 → SUMO 순서로 준비 상태와 메모리를 확인합니다.

아직 하지 않은 항목은 실제 Kubernetes 전체 전송·저장·조회 검증, SUMO 60초 운행의 기대 이벤트 ID와 MongoDB 비교, ACK 유실 및 Pod/DB 장애 복구, 지속 처리량·실제 Pod 메모리 측정입니다. Kafka 단일 브로커의 저장장치 손실이나 PC 전체 장애에서 무손실을 보장하지 않습니다.

## 관련 파일

- [Docker Hub 게시용 한·영 상세 설명](docker-hub-description.md)
- [Kubernetes 운영 가이드](kubernetes.md)
- [배포 스크립트](../scripts/deploy_kubernetes.ps1)
- [가벼운 자원 조회](../scripts/inspect_kubernetes_resources.ps1)
- [작은 데이터 흐름 검증](../scripts/verify_kubernetes_delivery.py)

## 추가 진행: 앱 정리와 Kafka 기동 확인

사용자가 사용하지 않는 앱을 내려달라고 요청하여 Chrome·VS Code에는 정상 종료를 요청하고 백그라운드 Slack을 종료했습니다. 실제 종료를 확인했습니다. 여유 RAM은 약 540MB에서 1.45GB로 늘었습니다.

이 여유로 laptop 배포를 재개했습니다. Kafka 4.3.1 이미지 다운로드와 초기화 후 `kafka-0`이 1/1 Running이 되었고 Kafka 로그의 서버 시작을 확인했습니다. `data-kafka-0` PVC는 Bound였으며 kubelet 통계에서 Kafka 컨테이너 working memory 약 296MB, CPU 약 0.10코어를 관찰했습니다. 지속 부하 측정은 아닙니다.

Kafka 준비 후 Windows 여유 RAM은 625MB였고 다음 MongoDB 단계의 보호 기준은 1,024MB여서 확대를 중단했습니다. MongoDB·Redis·서버·SUMO Pod는 배포하지 않았습니다. 이미지 다운로드 후 파일 캐시도 회수했으나 즉시 충분한 Windows 여유 메모리가 나오지 않았습니다.

이후 사용자의 Kubernetes 전체 실험 보류 의사를 반영하여 Kafka를 replicas 0으로 축소했고 Pod 삭제와 PVC 유지를 확인했습니다. Docker Desktop을 `docker desktop stop --timeout 60`으로 정상 종료했습니다. Docker·WSL·Chrome·VS Code·Slack 프로세스가 없는 상태에서 Windows 여유 RAM은 3,262,936KB, 약 3.1GiB였습니다. 이 채팅과 시스템 프로세스는 유지했습니다.

지금은 실행 서비스가 정지되어 있습니다. 전체 흐름·장애 복구 검증을 완료한 상태는 아니며, 사양만으로 Kubernetes 실행 불가능을 확정한 결과도 아닙니다. 다시 시작할 때 Docker Desktop과 클러스터 준비를 확인하고 laptop 프로필을 메모리 보호 조건 아래 재개합니다. 기존 Kafka PVC를 삭제하거나 초기화하지 않습니다.

> 이 문서는 커밋 `9ff798d`의 이전 Dispatcher·Pub/Sub 구조를 검증한 기록입니다. 현재 Kafka 단일 수신 구조는 [후속 검증](verification-2026-10-06-kafka-only.md)을 참고하세요.

# 실제 파이프라인 검증 — 2026-10-06

Tesla 공개 레포의 명칭을 참고한 Python 구현을 실제 SUMO 1.27.1, Python 3.13.11, Kafka 4.3.1 브로커 3개, Redis 8.2로 검증했습니다. Redis만 모의 구현으로 바꾼 결과가 아닙니다.

## 결과

| 검증 | 확인한 결과 |
|---|---|
| SUMO → 차량 SQLite → 차량별 WebSocket → Kafka | 실제 SUMO 선택 기록 4건; 차량 ID를 Kafka key로 사용; Kafka ACK 후 버퍼 0건 |
| Redis Producer | 차량별 Pub/Sub 4건 수신 |
| Logger Producer | 실제 콘솔 writer 출력 확인 |
| 조회 API | 실제 Redis 캐시 조회 HTTP 200 |
| Redis 컨테이너 중단 | Kafka 적재·차량 ACK 계속; 조회 API 503; 조회 Consumer 일시 정지 |
| Redis 재시작 | 미완료 Kafka 오프셋부터 처리해 최신 캐시 갱신 |
| 과거 기록 재전송 | 이력에는 저장, 최신 속도 20m/s 유지 |
| Kafka 3개 브로커 모두 중단 | SUMO 계속 수집; 차량 SQLite 미전송 5건 보존 |
| Kafka 재시작 | 검증 대상 이력 11건 모두 저장; 버퍼 0건; 최신 속도 30m/s |
| 연결·오류 기록 | 연결 토픽과 오류 토픽에서 각각 수신 |
| 일반 화면 실행 | 8092 포트의 화면·health·목록 HTTP 200; 실제 SUMO 차량 veh0·veh1 조회 |

전체 Python 테스트 94개와 Node의 화면 최신 상태 비교 테스트도 통과했습니다. 한 회귀 실행은 Docker 최초 시작 부하 중 3초 대기 제한에 걸렸으며, 해당 Kafka 테스트 17개와 전체 94개를 재실행해 모두 통과했습니다.

차량 관측 데이터는 실제 SUMO에서 만들었고, 장애 중 신규 전송·과거 데이터 역전 사례는 추가 합성 기록으로 확인했습니다. 차량 내부 버퍼 한도를 초과하는 규모·장시간 부하·디스크 고장은 이 검증 범위에 포함하지 않았습니다. 연결·오류 기록은 선택 목적지이므로 Kafka 장애 중 생성한 기록의 보존을 보장하지 않습니다. 오류 토픽 분배는 Kafka 복구 후 검증 오류를 새로 만들어 확인했습니다.

## 재현

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
docker compose up -d
.\.venv\Scripts\python.exe scripts\verify_live_pipeline.py --fault-injection
```

`--fault-injection`은 vol.2 Compose 프로젝트의 Redis와 Kafka 서비스를 실제로 중단·재시작합니다. 실행 전 컨테이너의 프로젝트·경로를 확인하며 다른 프로젝트면 거부합니다. 임시 SQLite, 별도 namespace·캐시 키·Consumer 그룹·토픽을 사용하고, 정상 검증 후 전용 토픽·캐시 키를 정리합니다. 기본 `data/event_buffer.db`는 사용하지 않습니다. 기존 Kafka/Redis 볼륨은 삭제하지 않습니다. 로컬 원시 결과는 Git에서 제외되는 `data/live-verification.json`과 로그에 있습니다.

수신 서버는 실제 Tesla 바이너리·인증 프로토콜과 호환되지 않습니다. 공개 명칭과 역할을 Python/JSON 모방에 적용한 검증입니다.

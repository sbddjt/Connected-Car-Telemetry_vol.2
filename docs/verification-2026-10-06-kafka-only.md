# Kafka 단일 수신 구조 검증 — 2026-10-06

검증 시각: 2026-10-06T22:33:16.053743+09:00 (한국 시간)

Dispatcher·Redis/Logger Producer·Pub/Sub/SSE를 제거한 구조를 검증했습니다. 수신 서버는 Kafka Producer를 직접 호출하며, 이력 Consumer와 조회 Consumer는 독립 Kafka 그룹으로 처리합니다. 이 문서는 이전 Dispatcher 구조 검증과 구분합니다.

## 자동 테스트

- Python unittest 80개 통과 (30.408초): Kafka 최종 성공 전 ACK 보류, 미확인 SQLite 보관, ACK 유실·중복, 재접속, 이력·Redis의 저장 후 오프셋 커밋, 최신 상태 되돌림 방지, Redis 조회 API.
- Node `tests/test_vehicle_state.js` 통과, `frontend/dashboard.js` 구문 검사 통과.
- `docker compose config --quiet` 통과.
- 삭제된 라우팅 설정 거부, 수신 서버에서 Redis 접속 옵션 제거, `/api/stream` 제거도 확인했습니다.

## 실제 서비스 검증

실제 SUMO headless, 차량 SQLite, WebSocket, Kafka 3개 브로커, 이력 SQLite와 Redis를 사용했습니다. 기존 차량 버퍼 대신 임시 SQLite, 별도 검증 토픽·Consumer Group·Redis 키를 사용하고 종료 시 검증용 리소스를 정리했습니다.

| 검증 | 결과 |
|---|---|
| SUMO → 차량 SQLite → 수신 서버 → Kafka → 이력 DB | 4개 선택 기록, 1대, ACK 후 차량 버퍼 0개 |
| Kafka 조회 Consumer → Redis → HTTP 조회 | HTTP 200 |
| 제거한 Pub/Sub/SSE API | HTTP 404 |
| Redis 중단 | Kafka ACK·이력 저장 계속, 조회 Consumer 일시 정지, 조회 API HTTP 503 |
| Redis 재시작 | 미커밋 기록 재처리·최신 상태 갱신 성공 |
| 뒤늦은 과거 기록 | 이력 저장 성공, Redis 최신 속도 20m/s 유지 |
| Kafka 전체 중단 | SUMO 수집 계속, 미확인 기록 5개 차량 버퍼에 보관 |
| Kafka 재시작 | 예상 전체 이력 11개 저장, 버퍼 0개, 최신 속도 30m/s |
| 연결·검증 오류 관측 기록 | connectivity/errors 토픽 수신 확인 |

장애 주입 중 출력되는 Kafka 연결 실패·전송 시간 초과와 재시도 로그는 예상 동작입니다. 연결·오류 관측 기록은 영속 버퍼가 없는 메모리 작업이므로 Kafka 중단 중 유실될 수 있습니다. 차량 기록은 Kafka 성공 ACK 전까지 차량 SQLite에 남습니다. 검증 후 Kafka·Redis 컨테이너를 모두 복구했습니다.

## 재현

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
node tests\test_vehicle_state.js
node --check frontend/dashboard.js
docker compose config --quiet
.\.venv\Scripts\python.exe scripts\verify_live_pipeline.py --fault-injection --report data/live-verification-kafka-only.json
```

`--fault-injection`은 vol.2 Compose의 Kafka·Redis를 일시 중단하고 다시 시작합니다. 스크립트가 Compose 프로젝트 이름·경로를 검사하여 다른 프로젝트의 컨테이너를 중단하지 않습니다. 실제 원시 결과와 로그는 `data/` 아래에 남고 Git에 포함하지 않습니다.

현재 화면은 Redis 최신 좌표·속도를 조회하는 차량 목록입니다. 지도 마커 및 실제 차량 GPS 장치 연결은 이번 검증 범위에 포함되지 않습니다.

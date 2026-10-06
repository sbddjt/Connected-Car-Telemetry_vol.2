# 강남 차량 관제 지도 검증 · 2026-10-06

## 구현 범위

- 기존 조회 API(8092)에서 Leaflet 1.9.4 지도·차량 마커·관제 화면 제공.
- OpenStreetMap 배경 지도, 기존 SUMO 도로망에서 추출한 로컬 GeoJSON 304개 도로.
- 차량 선택·ID 검색·최근 관측 필터·속도(km/h)·신호별 관측 시각·선택 차량 따라가기.
- 최대 800ms 위치 보간. 새 실행·5초 초과 공백·150m 초과 변화는 즉시 이동.
- 화면에서 받은 최근 80개 위치만 궤적으로 표시. 전체 DB 운행 이력 조회 기능은 별도 개발 대상.
- 데이터 경로는 차량 SQLite → 수신 서버 → Kafka → 별도 이력/Redis Consumer → 조회 API 그대로 유지.
- 차량 SQLite는 내부 영속 버퍼, 서버 SQLite는 향후 별도 DB로 교체 가능한 실험용 이력 저장소.

## 자동 검증

- Python unittest: **81개 통과**. 정적 지도 자산 제공, GeoJSON 좌표·도로 검증, 타일 이미지 CSP와 경로 접근 제한 포함.
- Node: 최신 상태 순서 비교와 보간 조건 테스트 통과.
- Chromium 실제 브라우저: 검색·차량 선택·최근 필터·m/s→km/h·위치 보간·늦은 과거 데이터 방지·503 중 위치 유지와 복구·궤적 초기화 통과.
- 화면 폭 1,440 / 1,024 / 768 / 390px: 가로 넘침 없음. 지도 제목·도구 겹침 없음. 데스크톱·모바일 전체 화면 캡처 후 육안 확인.
- 실제 실행 중 SUMO → 차량 버퍼 → WebSocket → Kafka 3개 브로커 → Redis → 브라우저에서 2.6초 동안 **14대의 위치 변화** 확인.
- JS 실행 오류 없음. 자동 브라우저 검증에서는 외부 지도 타일 요청을 차단하고 로컬 도로망 대체 화면을 확인함. 이 검증은 OSM 배경 타일 다운로드 성공을 의미하지 않음.
- 기존 차량 버퍼를 소비하지 않고 `data/runtime/demo_vehicle_buffer.db` 사용. 원본 Connected-Car-Telemetry 저장소는 변경하지 않음.

## 재현

README의 로컬 실행 명령으로 Kafka·Redis와 30분 SUMO 시연을 먼저 실행합니다.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -q
node tests\test_vehicle_state.js
node tests\test_map_motion.js
node scripts\verify_map_dashboard.cjs
```

브라우저 검증은 Playwright 및 Chromium이 설치된 개발 환경에서 실행합니다. 별도로 설치된 Playwright를 쓰면 `SERVER_MAP_QA_PLAYWRIGHT_MODULE_PATH`에 해당 모듈의 절대 경로를 지정합니다. API 주소는 `SERVER_QUERY_API_TEST_URL`로 변경할 수 있으며 기본값은 `http://127.0.0.1:8092/`입니다. 실시간 SUMO 차량 이동도 확인하므로 시뮬레이션이 실행 중이어야 합니다.

검증 중 UI 동작은 브라우저 응답을 대체해 확인하며, 마지막 실시간 확인은 실제 API를 사용합니다. DB·Kafka·Redis 기록을 직접 주입하지 않습니다. 화면 캡처와 시연 DB·로그는 `data/runtime/`에 두고 Git에 포함하지 않습니다.

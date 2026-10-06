"""서버의 운행 이력과 신호별 최신 상태 저장. 차량 버퍼와 별도 DB입니다."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from shared_fleet_telemetry_policy import signals_of, utc_timestamp, validate_event


class ServerTelemetryStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_history (
                    event_id TEXT PRIMARY KEY,
                    vehicle_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
            """)
            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_history_vehicle_time
                ON telemetry_history (vehicle_id, event_time)
            """)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS vehicle_latest_state (
                    vehicle_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    value TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    sequence_no INTEGER NOT NULL,
                    PRIMARY KEY (vehicle_id, signal)
                )
            """)

    @staticmethod
    def _identity(event):
        value = dict(event)
        value.pop("received_at", None)
        return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)

    def save(self, event: dict):
        validate_event(event)
        stamp = utc_timestamp(event["event_time"])
        received_at = utc_timestamp(event.get("received_at") or datetime.now(timezone.utc).isoformat())
        payload = json.dumps(event, ensure_ascii=False, allow_nan=False)
        with self.connection:
            existing = self.connection.execute(
                "SELECT payload FROM telemetry_history WHERE event_id = ?", (event["event_id"],)
            ).fetchone()
            if existing:
                if self._identity(json.loads(existing[0])) != self._identity(event):
                    raise ValueError("Conflicting payload for the same event_id")
                return  # 동일 이벤트 재전송으로 이력과 상태를 중복 갱신하지 않습니다.
            self.connection.execute(
                "INSERT INTO telemetry_history VALUES (?, ?, ?, ?, ?)",
                (event["event_id"], event["vehicle_id"], stamp, received_at, payload),
            )
            for signal, value in signals_of(event).items():
                self.connection.execute("""
                    INSERT INTO vehicle_latest_state VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(vehicle_id, signal) DO UPDATE SET
                        value = excluded.value, event_time = excluded.event_time,
                        run_id = excluded.run_id, sequence_no = excluded.sequence_no
                    WHERE excluded.event_time > vehicle_latest_state.event_time
                       OR (excluded.event_time = vehicle_latest_state.event_time
                           AND excluded.run_id = vehicle_latest_state.run_id
                           AND excluded.sequence_no > vehicle_latest_state.sequence_no)
                """, (
                    event["vehicle_id"], signal, json.dumps(value, allow_nan=False),
                    stamp, event["run_id"], event["sequence_no"],
                ))

    def close(self):
        self.connection.close()

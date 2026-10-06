import json
import sqlite3
from pathlib import Path

from shared_fleet_telemetry_policy import signals_of


class VehicleSQLiteBuffer:
    def __init__(self, db_path: Path, max_events: int = 5_000, per_vehicle: bool = True):
        if max_events < 1:
            raise ValueError("max_events must be at least 1")

        self.max_events = max_events
        self.per_vehicle = per_vehicle
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(
            db_path,
            autocommit=sqlite3.LEGACY_TRANSACTION_CONTROL,
        )

        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")

        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    payload TEXT NOT NULL,
                    delivered INTEGER NOT NULL DEFAULT 0
                        CHECK (delivered IN (0, 1))
                )
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_pending
                ON events (delivered, id)
            """)

            # 차량별 최신 미전송 행을 찾기 위한 인덱스입니다.
            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_vehicle_pending
                ON events (delivered, json_extract(payload, '$.vehicle_id'), id)
            """)

            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_vehicle_capacity
                ON events (json_extract(payload, '$.vehicle_id'), delivered, id)
            """)

            # 용량 초과로 삭제한 미전송 이벤트 수를 재시작 후에도 유지합니다.
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS buffer_stats (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                )
            """)
            self.connection.execute("""
                INSERT OR IGNORE INTO buffer_stats (name, value)
                VALUES ('capacity_dropped', 0)
            """)

    def save(self, event: dict) -> None:
        payload = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

        dropped = 0

        # 신규 저장, 기존 데이터 정리, 드롭 집계를 함께 커밋합니다.
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO events (event_id, payload)
                VALUES (?, ?)
                """,
                (event["event_id"], payload),
            )

            scope = "json_extract(payload, '$.vehicle_id') = ?" if self.per_vehicle else "1 = 1"
            scope_args = (event["vehicle_id"],) if self.per_vehicle else ()
            count = self.connection.execute(
                f"SELECT COUNT(*) FROM events WHERE {scope}", scope_args
            ).fetchone()[0]
            overflow = count - self.max_events

            if overflow > 0:
                # 완료 데이터를 먼저, 같은 상태에서는 저장된 순서로 정리합니다.
                victims = self.connection.execute(
                    f"""
                    SELECT id, delivered
                    FROM events WHERE {scope}
                    ORDER BY delivered DESC, id ASC
                    LIMIT ?
                    """,
                    (*scope_args, overflow),
                ).fetchall()

                # 완료 데이터 정리는 드롭으로 집계하지 않습니다.
                dropped = sum(
                    delivered == 0
                    for _, delivered in victims
                )

                self.connection.executemany(
                    "DELETE FROM events WHERE id = ?",
                    [(row_id,) for row_id, _ in victims],
                )

                if dropped:
                    self.connection.execute(
                        """
                        UPDATE buffer_stats
                        SET value = value + ?
                        WHERE name = 'capacity_dropped'
                        """,
                        (dropped,),
                    )

        # 커밋 성공 후에만 드롭 사실을 출력합니다.
        if dropped:
            print(f"[버퍼 용량 초과] 미전송 이벤트 {dropped}건 삭제")

    @staticmethod
    def _exclude_clause(exclude_ids: set[str]) -> tuple[str, tuple]:
        if not exclude_ids:
            return "1 = 1", ()
        ids = tuple(sorted(exclude_ids))
        placeholders = ",".join("?" for _ in ids)
        return f"event_id NOT IN ({placeholders})", ids

    def get_pending_records(
        self, limit: int = 100, exclude_ids: set[str] | None = None,
        vehicle_id: str | None = None,
    ) -> list[tuple[int, dict]]:
        """전송 중인 이벤트를 제외하고 오래된 기록부터 조회합니다."""
        clause, ids = self._exclude_clause(exclude_ids or set())
        if vehicle_id is not None:
            clause += " AND json_extract(payload, '$.vehicle_id') = ?"
            ids += (vehicle_id,)
        rows = self.connection.execute(
            f"""
            SELECT id, payload FROM events
            WHERE delivered = 0 AND {clause}
            ORDER BY id LIMIT ?
            """,
            (*ids, limit),
        ).fetchall()
        return [(row_id, json.loads(payload)) for row_id, payload in rows]

    def get_latest_pending(
        self,
        limit: int = 100,
        exclude_ids: set[str] | None = None,
        acknowledged_rows: dict | None = None,
        vehicle_id: str | None = None,
    ) -> list[tuple[int, dict]]:
        """신호별 최신 미전송 행을 조회합니다. 이전 스냅샷 형식도 지원합니다."""
        if limit < 1:
            return []
        clause, ids = self._exclude_clause(exclude_ids or set())
        acknowledged_rows = acknowledged_rows or {}
        vehicle_clause = "AND json_extract(e.payload, '$.vehicle_id') = ?" if vehicle_id is not None else ""
        vehicle_args = (vehicle_id,) if vehicle_id is not None else ()
        rows = self.connection.execute(
            f"""
            SELECT id, payload FROM events
            WHERE id IN (
                SELECT MAX(e.id) FROM events e, json_each(e.payload, '$.signals') signal
                WHERE e.delivered = 0 {vehicle_clause}
                GROUP BY json_extract(e.payload, '$.vehicle_id'), signal.key
                UNION
                SELECT MAX(e.id) FROM events e
                WHERE e.delivered = 0 AND json_type(e.payload, '$.signals') IS NULL {vehicle_clause}
                GROUP BY json_extract(e.payload, '$.vehicle_id')
            ) AND {clause}
            ORDER BY id DESC
            """,
            (*vehicle_args, *vehicle_args, *ids),
        )
        result = []
        for row_id, payload in rows:
            event = json.loads(payload)
            signal_names = signals_of(event) or {"snapshot": None}
            if all(row_id <= acknowledged_rows.get(
                (event["vehicle_id"], signal), acknowledged_rows.get(event["vehicle_id"], 0)
            ) for signal in signal_names):
                continue
            result.append((row_id, event))
            if len(result) >= limit:
                break
        return result

    def pending_vehicles(self) -> list[str]:
        return [row[0] for row in self.connection.execute(
            "SELECT DISTINCT json_extract(payload, '$.vehicle_id') FROM events WHERE delivered = 0"
        )]

    def get_pending(self, limit: int = 100) -> list[dict]:
        return [event for _, event in self.get_pending_records(limit)]

    def acknowledge(self, event_id: str) -> None:
        """Kafka 성공 확인을 받은 이벤트만 로컬 버퍼에서 삭제합니다."""
        with self.connection:
            self.connection.execute(
                "DELETE FROM events WHERE event_id = ?",
                (event_id,),
            )

    def cleanup_completed(self) -> None:
        """이전 버전이 전송 완료로 표시한 데이터를 정리합니다."""
        with self.connection:
            self.connection.execute("DELETE FROM events WHERE delivered = 1")

    def close(self) -> None:
        self.connection.close()

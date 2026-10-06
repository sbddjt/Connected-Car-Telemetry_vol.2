import json
import sqlite3
from pathlib import Path


class EventBuffer:
    def __init__(self, db_path: Path):
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

    def save(self, event: dict) -> None:
        payload = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

        with self.connection:
            self.connection.execute(
                """
                INSERT INTO events (event_id, payload)
                VALUES (?, ?)
                """,
                (event["event_id"], payload),
            )

    def get_pending(self, limit: int = 100) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT payload
            FROM events
            WHERE delivered = 0
            ORDER BY id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [json.loads(row[0]) for row in rows]

    def mark_delivered(self, event_id: str) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE events
                SET delivered = 1
                WHERE event_id = ?
                """,
                (event_id,),
            )

    def close(self) -> None:
        self.connection.close()
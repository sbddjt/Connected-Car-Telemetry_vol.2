import contextlib
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vehicle_sqlite_buffer import VehicleSQLiteBuffer


class VehicleSQLiteBufferPolicyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="telemetry-buffer-policy-")
        temp_path = Path(self.directory.name).resolve()
        self.assertTrue(temp_path.is_relative_to(Path(tempfile.gettempdir()).resolve()))
        self.path = temp_path / "buffer.db"
        self.buffer = VehicleSQLiteBuffer(self.path, max_events=3)
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(lambda: self.buffer.close())

    def save(self, event_id):
        with contextlib.redirect_stdout(io.StringIO()):
            self.buffer.save({"event_id": event_id, "vehicle_id": "car-1"})

    def seed(self):
        for event_id in ("a", "b", "c"):
            self.save(event_id)

    def stored_ids(self):
        return [
            row[0] for row in self.buffer.connection.execute(
                "SELECT event_id FROM events ORDER BY id"
            )
        ]

    def dropped(self):
        return self.buffer.connection.execute(
            "SELECT value FROM buffer_stats WHERE name = 'capacity_dropped'"
        ).fetchone()[0]

    def test_completed_event_removed_before_older_pending(self):
        self.seed()
        # 이전 버전의 완료 행이 남아 있는 DB를 재현합니다.
        with self.buffer.connection:
            self.buffer.connection.execute(
                "UPDATE events SET delivered = 1 WHERE event_id = ?", ("b",)
            )
        self.save("d")
        self.assertEqual(self.stored_ids(), ["a", "c", "d"])
        self.assertEqual(self.dropped(), 0)

    def test_oldest_pending_dropped_and_latest_retained(self):
        self.seed()
        self.save("d")
        self.assertEqual(self.stored_ids(), ["b", "c", "d"])
        self.assertEqual(self.dropped(), 1)
        self.assertEqual(
            [event["event_id"] for event in self.buffer.get_pending()],
            ["b", "c", "d"],
        )

    def test_duplicate_insert_keeps_events_and_counter(self):
        self.seed()
        with self.assertRaises(sqlite3.IntegrityError):
            self.save("c")
        self.assertEqual(self.stored_ids(), ["a", "b", "c"])
        self.assertEqual(self.dropped(), 0)

    def test_failure_during_cleanup_rolls_back_insert_and_deletion(self):
        self.seed()
        self.buffer.connection.execute("""
            CREATE TRIGGER fail_stats BEFORE UPDATE ON buffer_stats
            BEGIN
                SELECT RAISE(ABORT, 'forced test failure');
            END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.save("d")
        self.assertEqual(self.stored_ids(), ["a", "b", "c"])
        self.assertEqual(self.dropped(), 0)

    def test_pending_events_and_drop_count_survive_reopen(self):
        self.seed()
        self.save("d")
        self.buffer.close()
        self.buffer = VehicleSQLiteBuffer(self.path, max_events=3)
        self.assertEqual(self.stored_ids(), ["b", "c", "d"])
        self.assertEqual(self.dropped(), 1)

    def test_existing_database_gets_stats_table_without_resetting_events(self):
        self.seed()
        with self.buffer.connection:
            self.buffer.connection.execute("DROP TABLE buffer_stats")
        self.buffer.close()
        self.buffer = VehicleSQLiteBuffer(self.path, max_events=2)
        self.assertEqual(self.stored_ids(), ["a", "b", "c"])
        self.save("d")
        self.assertEqual(self.stored_ids(), ["c", "d"])
        self.assertEqual(self.dropped(), 2)

    def test_acknowledge_deletes_only_confirmed_event(self):
        self.seed()
        self.buffer.acknowledge("b")
        self.assertEqual(self.stored_ids(), ["a", "c"])
        self.assertEqual(self.dropped(), 0)
        self.buffer.acknowledge("b")
        self.assertEqual(self.stored_ids(), ["a", "c"])

    def test_cleanup_completed_preserves_pending(self):
        self.seed()
        with self.buffer.connection:
            self.buffer.connection.execute(
                "UPDATE events SET delivered = 1 WHERE event_id = ?", ("b",)
            )
        self.buffer.cleanup_completed()
        self.assertEqual(self.stored_ids(), ["a", "c"])
        self.assertEqual(self.dropped(), 0)

    def test_latest_query_selects_one_per_vehicle_and_excludes_outstanding(self):
        self.buffer.max_events = 10
        for event_id, car in (("a1", "a"), ("b1", "b"), ("a2", "a"), ("b2", "b")):
            self.buffer.save({"event_id": event_id, "vehicle_id": car})
        self.assertEqual(
            [e["event_id"] for _, e in self.buffer.get_latest_pending()], ["b2", "a2"]
        )
        # 최신 행이 전송 중이어도 이전 행을 최신 위치로 취급하지 않습니다.
        self.assertEqual(
            [e["event_id"] for _, e in self.buffer.get_latest_pending(exclude_ids={"a2"})],
            ["b2"],
        )
        self.assertEqual(
            [e["event_id"] for _, e in self.buffer.get_pending_records(exclude_ids={"a2", "b2"})],
            ["a1", "b1"],
        )

    def test_acknowledged_rows_do_not_hide_other_vehicles(self):
        self.buffer.max_events = 10
        for number in range(1, 5):
            self.buffer.save({"event_id": str(number), "vehicle_id": str(number)})
        self.assertEqual(
            [e["event_id"] for _, e in self.buffer.get_latest_pending(
                limit=1, acknowledged_rows={"1": 1, "2": 2, "3": 3}
            )], ["4"],
        )
        self.assertEqual(self.buffer.get_latest_pending(limit=0), [])

    def test_existing_database_gets_vehicle_index_without_changing_rows(self):
        self.seed()
        self.buffer.connection.execute("DROP INDEX idx_events_vehicle_pending")
        self.buffer.close()
        self.buffer = VehicleSQLiteBuffer(self.path)
        self.assertEqual(self.stored_ids(), ["a", "b", "c"])
        self.assertEqual(
            [e["event_id"] for _, e in self.buffer.get_latest_pending()], ["c"]
        )

    def test_invalid_limit_rejected(self):
        with self.assertRaises(ValueError):
            VehicleSQLiteBuffer(self.path, max_events=0)

    def test_invalid_payload_keeps_existing_events(self):
        self.seed()
        with self.assertRaises(ValueError):
            self.buffer.save({"event_id": "d", "speed": float("nan")})
        self.assertEqual(self.stored_ids(), ["a", "b", "c"])
        self.assertEqual(self.dropped(), 0)


if __name__ == "__main__":
    unittest.main()

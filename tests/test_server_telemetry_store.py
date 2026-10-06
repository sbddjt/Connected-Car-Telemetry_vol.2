import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from server_telemetry_store import ServerTelemetryStore
from server_telemetry_consumer import persist_and_commit
from test_server_kafka_producer import event


class ServerTelemetryStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = ServerTelemetryStore(Path(self.directory.name) / "server.db")
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(self.store.close)

    def latest(self):
        return {
            signal: json.loads(value)
            for signal, value in self.store.connection.execute(
                "SELECT signal, value FROM vehicle_latest_state WHERE vehicle_id='car-1'"
            )
        }

    def count(self):
        return self.store.connection.execute("SELECT COUNT(*) FROM telemetry_history").fetchone()[0]

    def test_late_history_stored_without_rewinding_location_or_speed(self):
        self.store.save(event(5))
        self.store.save(event(1))
        self.store.save(event(3))
        self.assertEqual(self.count(), 3)
        self.assertEqual(self.latest(), event(5)["signals"])
        self.store.save(event(6))
        self.assertEqual(self.latest(), event(6)["signals"])

    def test_partial_signal_updates_keep_independent_observation_times(self):
        self.store.save(event(5, signals={"location": {"latitude": 37.5, "longitude": 127.5}}))
        self.store.save(event(3, signals={"speed_mps": 30}))
        self.store.save(event(2))
        self.assertEqual(self.latest(), {
            "location": {"latitude": 37.5, "longitude": 127.5}, "speed_mps": 30,
        })
        self.assertEqual(self.count(), 3)

    def test_replayed_id_deduplicates_even_if_received_at_changes(self):
        value = event(5)
        value["received_at"] = "2026-10-06T00:00:06Z"
        self.store.save(value)
        value["received_at"] = "2026-10-06T00:01:00Z"
        self.store.save(value)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.latest(), value["signals"])

    def test_duplicate_id_with_conflicting_data_is_rejected(self):
        self.store.save(event())
        conflict = event()
        conflict["signals"]["speed_mps"] = 99
        with self.assertRaises(ValueError):
            self.store.save(conflict)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.latest(), event()["signals"])

    def test_same_time_uses_sequence_only_within_the_same_run(self):
        first = event(1)
        newer = event(2)
        newer["event_time"] = first["event_time"]
        self.store.save(first)
        self.store.save(newer)
        self.assertEqual(self.latest(), newer["signals"])
        other_run = event(3)
        other_run["event_time"] = first["event_time"]
        other_run["run_id"] = "new-run"
        self.store.save(other_run)
        self.assertEqual(self.latest(), newer["signals"])

    def test_timestamp_formats_are_normalized_before_comparison(self):
        first = event(1)
        first["event_time"] = "2026-10-06T09:00:01+09:00"
        second = event(2)
        second["event_time"] = "2026-10-06T00:00:01.001Z"
        self.store.save(second)
        self.store.save(first)
        self.assertEqual(self.latest(), second["signals"])

    def test_failure_rolls_back_history_and_does_not_commit_kafka_offset(self):
        self.store.connection.execute("""
            CREATE TRIGGER fail_state BEFORE INSERT ON vehicle_latest_state
            BEGIN SELECT RAISE(ABORT, 'test state failure'); END
        """)
        message = Mock()
        message.value.return_value = json.dumps(event()).encode()
        consumer = Mock()
        with self.assertRaises(sqlite3.IntegrityError):
            persist_and_commit(message, self.store, consumer)
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.latest(), {})
        consumer.commit.assert_not_called()

    def test_offset_commit_after_durable_save_can_be_retried_safely(self):
        message = Mock()
        message.value.return_value = json.dumps(event()).encode()
        consumer = Mock()
        consumer.commit.side_effect = RuntimeError("test commit failure")
        with self.assertRaises(RuntimeError):
            persist_and_commit(message, self.store, consumer)
        self.assertEqual(self.count(), 1)
        consumer.commit.side_effect = None
        persist_and_commit(message, self.store, consumer)
        self.assertEqual(self.count(), 1)
        consumer.commit.assert_called_with(message=message, asynchronous=False)


if __name__ == "__main__":
    unittest.main()

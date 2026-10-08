import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vehicle_sqlite_buffer import VehicleSQLiteBuffer, BufferCapacityExceeded
from vehicle_sumo_collector import save_collected_batch, parse_args

class BufferBackpressureTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "buffer.db"
        self.buffer = VehicleSQLiteBuffer(self.path, max_events=2, overflow_policy="block")
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(lambda: self.buffer.close())

    def event(self, identifier, car="car-1"):
        return {"event_id": identifier, "vehicle_id": car}

    def ids(self):
        return [row["event_id"] for row in self.buffer.get_pending()]

    def test_full_buffer_rolls_back_new_batch_and_preserves_old_records_after_restart(self):
        self.buffer.save_many([self.event("a"), self.event("b")])
        with self.assertRaises(BufferCapacityExceeded):
            self.buffer.save_many([self.event("other", "car-2"), self.event("c")])
        self.buffer.close()
        self.buffer = VehicleSQLiteBuffer(self.path, max_events=2, overflow_policy="block")
        self.assertEqual(self.ids(), ["a", "b"])
        self.assertEqual(self.buffer.connection.execute(
            "SELECT value FROM buffer_stats WHERE name='capacity_dropped'").fetchone()[0], 0)

    def test_collection_retries_identical_batch_after_ack_creates_space(self):
        self.buffer.save_many([self.event("a"), self.event("b")])
        with patch("vehicle_sumo_collector.time.sleep", side_effect=lambda _: self.buffer.acknowledge("a")) as sleep:
            save_collected_batch(self.buffer, [self.event("c")])
        self.assertEqual(sleep.call_count, 1)
        self.assertEqual(self.ids(), ["b", "c"])

    def test_capacity_is_independent_per_vehicle(self):
        self.buffer.save_many([self.event("a"), self.event("b")])
        self.buffer.save_many([self.event("other", "car-2")])
        self.assertEqual(self.ids(), ["a", "b", "other"])

    def test_legacy_completed_rows_can_be_cleaned_without_dropping_pending(self):
        self.buffer.save_many([self.event("a"), self.event("b")])
        with self.buffer.connection:
            self.buffer.connection.execute("UPDATE events SET delivered=1 WHERE event_id='a'")
        self.buffer.save(self.event("c"))
        self.assertEqual(self.ids(), ["b", "c"])

    def test_block_policy_rejects_sharded_collection(self):
        with self.assertRaises(SystemExit):
            parse_args(["--vehicle-buffer-overflow-policy", "block", "--vehicle-buffer-directory", "data/test"])

if __name__ == "__main__":
    unittest.main()

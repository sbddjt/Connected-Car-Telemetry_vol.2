import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from vehicle_fleet_telemetry_client import SimulatedFleetTelemetryClients, VehicleFleetTelemetryClient
from test_server_kafka_producer import event


class BatchAckTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.buffer = VehicleSQLiteBuffer(Path(directory.name) / "buffer.db")
        self.addCleanup(self.buffer.close)
        with patch("vehicle_fleet_telemetry_client.PipelineMetrics"):
            self.fleet = SimulatedFleetTelemetryClients(self.buffer)
        self.sender = VehicleFleetTelemetryClient(self.buffer, "car-1", defer_local_deletes=True)
        self.fleet.senders["car-1"] = self.sender
        self.buffer.save_many([event(1), event(2), event(3)])
        for row_id, record in self.buffer.get_pending_records(limit=3):
            self.sender.in_flight[record["event_id"]] = (row_id, record, 0)
        for number in (1, 2):
            self.sender._accept_ack({"vehicle_id": "car-1", "event_id": event(number)["event_id"]})

    def test_batch_delete_preserves_unacknowledged_record(self):
        self.assertEqual(len(self.buffer.get_pending()), 3)
        self.fleet._flush_acknowledged()
        self.assertEqual([record["event_id"] for record in self.buffer.get_pending()], [event(3)["event_id"]])
        self.assertFalse(self.sender.pending_acks)
        self.assertIn(event(3)["event_id"], self.sender.in_flight)

    def test_failed_batch_delete_keeps_acknowledgments_for_local_retry(self):
        with patch.object(self.buffer, "acknowledge_many", side_effect=sqlite3.OperationalError("locked")):
            self.fleet._flush_acknowledged()
        self.assertEqual(len(self.buffer.get_pending()), 3)
        self.assertEqual(len(self.sender.pending_acks), 2)
        for retry in self.sender.pending_acks.values():
            retry.after = 0
        self.fleet._flush_acknowledged()
        self.assertEqual(len(self.buffer.get_pending()), 1)
        self.assertFalse(self.sender.pending_acks)


if __name__ == "__main__":
    unittest.main()

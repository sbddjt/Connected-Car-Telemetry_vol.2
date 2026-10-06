import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock
from pymongo.errors import BulkWriteError, ConnectionFailure, WriteConcernError
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from server_mongodb_history_store import MongoDBHistoryStore
from test_server_kafka_producer import event


class MongoHistoryTests(unittest.TestCase):
    def setUp(self):
        self.store = MongoDBHistoryStore(client=MagicMock())
        self.collection = self.store.collection
        self.record = event()
        self.identity = json.dumps(self.record, sort_keys=True, ensure_ascii=False, allow_nan=False)

    def duplicate(self):
        return BulkWriteError({"writeErrors": [{"index": 0, "code": 11000}], "writeConcernErrors": []})

    def test_concurrent_identical_upsert_retries_successfully(self):
        self.collection.bulk_write.side_effect = [self.duplicate(), MagicMock()]
        self.collection.find_one.return_value = {"payload_identity": self.identity}
        self.store.save(self.record)
        self.assertEqual(self.collection.bulk_write.call_count, 2)

    def test_same_id_with_different_payload_is_rejected(self):
        self.collection.bulk_write.side_effect = self.duplicate()
        self.collection.find_one.return_value = {"payload_identity": "different"}
        with self.assertRaisesRegex(ValueError, "Conflicting history record"):
            self.store.save(self.record)
        self.assertEqual(self.collection.bulk_write.call_count, 1)

    def test_retry_received_time_does_not_change_vehicle_identity(self):
        self.collection.bulk_write.side_effect = [self.duplicate(), MagicMock()]
        self.collection.find_one.return_value = {"payload_identity": self.identity}
        self.store.save({**self.record, "received_at": "2026-10-07T00:00:00Z"})
        self.assertEqual(self.collection.bulk_write.call_count, 2)

    def test_repeated_upsert_race_remains_retryable(self):
        self.collection.bulk_write.side_effect = self.duplicate()
        self.collection.find_one.return_value = {"payload_identity": self.identity}
        with self.assertRaises(ConnectionFailure):
            self.store.save(self.record)
        self.assertEqual(self.collection.bulk_write.call_count, 3)

    def test_write_concern_failure_is_not_reported_as_storage_success(self):
        self.collection.bulk_write.side_effect = BulkWriteError({"writeErrors": [], "writeConcernErrors": [{"code": 64}]})
        with self.assertRaises(WriteConcernError):
            self.store.save(self.record)


if __name__ == "__main__":
    unittest.main()

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from telemetry.record import Record
from datastore.kafka.kafka import Producer as KafkaProducer
from server_fleet_telemetry import parse_args
from server_vehicle_query_api import parse_args as query_args
from test_server_kafka_producer import event, FakeKafka


class TeslaNamingTests(unittest.IsolatedAsyncioTestCase):
    def test_record_names_map_without_rewriting_json(self):
        data = event()
        entry = Record.from_event("V", data)
        self.assertEqual((entry.vin, entry.tx_type, entry.txid),
                         (data["vehicle_id"], "V", data["event_id"]))
        self.assertIs(entry.data, data)

    async def test_produce_record_uses_vin_as_kafka_key(self):
        native = FakeKafka()
        producer = KafkaProducer(producer_factory=native.factory)
        task = asyncio.create_task(producer.produce(Record.from_event("V", event())))
        await asyncio.sleep(0)
        self.assertEqual(native.queue[0]["key"], b"car-1")
        producer.poll()
        await task
        self.assertEqual(len(native.delivered), 1)
        producer.close()

    def test_password_precedence_applies_only_to_query_and_config_alias_is_kept(self):
        with patch.dict(os.environ, {"REDIS_PASSWORD": "canonical", "SERVER_REDIS_PASSWORD": "legacy"}):
            args = query_args([])
            self.assertEqual(args.redis_client_options["password"], "canonical")
            receiver = parse_args(["--config", "config/server_fleet_telemetry_config.json"])
            legacy = parse_args(["--server-telemetry-config", "config/server_fleet_telemetry_config.json"])
            self.assertEqual(receiver.server_telemetry_config, legacy.server_telemetry_config)
            self.assertFalse(hasattr(receiver, "redis_client_options"))


if __name__ == "__main__":
    unittest.main()

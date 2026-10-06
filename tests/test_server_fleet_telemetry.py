import asyncio
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from datastore.kafka.kafka import Producer as KafkaProducer
from server_fleet_telemetry import FleetTelemetryServer, parse_args
from shared_runtime_config import SERVER_TELEMETRY_CONFIG, load_server_telemetry_config
from telemetry.record import Record
from test_server_kafka_producer import FakeKafka, event, until
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


class ReceiverTests(unittest.IsolatedAsyncioTestCase):
    def make_receiver(self, **options):
        native = FakeKafka()
        producer = KafkaProducer(producer_factory=native.factory)
        receiver = FleetTelemetryServer(producer, **options)
        self.addCleanup(producer.close)
        self.addAsyncCleanup(receiver.close, timeout=0.05)
        return native, producer, receiver

    async def test_kafka_direct_delivery_waits_for_callback_before_ack(self):
        native, producer, receiver = self.make_receiver()
        native.hold = True
        async with serve(receiver.handle, "127.0.0.1", 0) as server:
            url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            async with connect(url, proxy=None) as client:
                await client.send(json.dumps({"type": "hello", "vehicle_id": "car-1"}))
                self.assertEqual(json.loads(await client.recv())["type"], "ready")
                await client.send(json.dumps({"type": "telemetry", "event": event()}))
                reply = asyncio.create_task(client.recv())
                await until(lambda: bool(native.vehicle_attempts))
                self.assertFalse(reply.done())
                native.hold = False
                while native.queue:
                    producer.poll()
                self.assertEqual(json.loads(await asyncio.wait_for(reply, 1))["type"], "ack")
                await client.send(json.dumps({"type": "telemetry", "event": event(vehicle="wrong")}))
                self.assertEqual(json.loads(await client.recv())["type"], "error")
            await until(lambda: len(native.attempts) >= 4)
        while native.queue:
            producer.poll()
        await receiver.close()
        self.assertEqual(len(native.vehicle_delivered), 1)
        operational = [record for record in native.delivered if "record_type" in record]
        self.assertEqual([record["record_type"] for record in operational],
                         ["connectivity", "errors", "connectivity"])
        self.assertEqual(operational[0]["connection_id"], operational[-1]["connection_id"])

    async def test_notification_limit_preserves_capacity_and_close_cancels_waits(self):
        native, producer, receiver = self.make_receiver(max_notification_tasks=2)
        native.hold = True
        for number in range(5):
            receiver._notify("errors", "car-1", "connection", error=str(number))
        self.assertEqual(len(receiver.notification_tasks), 2)
        self.assertEqual(receiver.notification_dropped, 3)
        await asyncio.sleep(0)
        await receiver.close(timeout=0.01)
        self.assertFalse(receiver.notification_tasks)
        receiver._notify("errors", "car-1", "connection")
        self.assertEqual(receiver.notification_dropped, 4)
        native.hold = False
        while native.queue:
            producer.poll()

    async def test_notification_failure_does_not_fail_vehicle_delivery(self):
        native, producer, receiver = self.make_receiver()
        native.online = False
        receiver._notify("errors", "car-1", "connection")
        await asyncio.sleep(0)
        with self.assertLogs("server_fleet_telemetry", level="WARNING"):
            producer.poll()
            await until(lambda: receiver.notification_failures == 1)
        native.online = True
        task = asyncio.create_task(producer.publish(event()))
        await asyncio.sleep(0)
        producer.poll()
        await task

    async def test_record_type_is_part_of_kafka_pending_identity(self):
        native, producer, receiver = self.make_receiver()
        vehicle = event()
        connectivity = {"event_id": vehicle["event_id"], "vehicle_id": vehicle["vehicle_id"],
                        "event_time": vehicle["event_time"], "status": "connected"}
        tasks = [asyncio.create_task(producer.produce(Record.from_event(kind, data)))
                 for kind, data in (("V", vehicle), ("connectivity", connectivity))]
        await asyncio.sleep(0)
        self.assertEqual(len(native.queue), 2)
        self.assertEqual({item["topic"] for item in native.queue},
                         {"vehicle-telemetry-v1", "vehicle-connectivity-v1"})
        while native.queue:
            producer.poll()
        await asyncio.gather(*tasks)
        self.assertEqual(producer.pending, {})


class ReceiverConfigurationTests(unittest.TestCase):
    def test_removed_routing_settings_and_invalid_topics_are_rejected(self):
        original = json.loads(SERVER_TELEMETRY_CONFIG.read_text(encoding="utf-8"))
        changes = [
            {"records": {"V": ["kafka"]}}, {"logger": {"verbose": False}},
            {"namespace": "unused"}, {"redis": {"publish_vin_topics": True}},
            {"reliable_ack_sources": {"V": "redis"}},
            {"kafka_topics": {"V": "same", "connectivity": "same", "errors": "other"}},
            {"kafka_topics": {"V": "vehicle-only"}},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.json"
            for change in changes:
                with self.subTest(change=change):
                    path.write_text(json.dumps({**original, **change}), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        load_server_telemetry_config(path)

    def test_role_named_environment_overrides_topics_without_redis_connection(self):
        with patch.dict(os.environ, {
            "SERVER_KAFKA_CONNECTIVITY_TOPIC": "custom-connectivity",
            "SERVER_KAFKA_ERRORS_TOPIC": "custom-errors",
            "SERVER_REDIS_URL": "invalid-unused-value",
        }, clear=True):
            args = parse_args(["--kafka-vehicle-topic", "custom-vehicle"])
        self.assertEqual(args.kafka_topics, {
            "V": "custom-vehicle", "connectivity": "custom-connectivity", "errors": "custom-errors",
        })
        self.assertFalse(hasattr(args, "redis_client_options"))

    def test_topic_overrides_cannot_mix_operations_with_vehicle_history(self):
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                parse_args(["--kafka-connectivity-topic", "vehicle-telemetry-v1"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()

import asyncio
import contextlib
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from server_dispatcher import DeliveryError, DispatchRouter
from server_fleet_telemetry import FleetTelemetryServer, build_dispatch_router, parse_args
from server_kafka_dispatcher import KafkaDispatcher
from server_logger_dispatcher import LoggerDispatcher
from shared_runtime_config import SERVER_TELEMETRY_CONFIG, load_server_telemetry_config
from test_server_kafka_dispatcher import FakeKafka, event, until
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


class RecordingDispatcher:
    def __init__(self, gate=None, error=None, mutate=False):
        self.records = []
        self.gate = gate
        self.error = error
        self.mutate = mutate

    async def dispatch(self, record_type, record):
        self.records.append((record_type, record))
        if self.mutate:
            record["signals"]["speed_mps"] = 999
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error


class DispatchRouterTests(unittest.IsolatedAsyncioTestCase):
    def make_router(self, kafka, logger, **settings):
        router = DispatchRouter(
            {"kafka": kafka, "logger": logger},
            {"V": ["kafka", "logger"], "errors": ["logger"]},
            {"V": "kafka"}, **settings,
        )
        self.addAsyncCleanup(router.close, timeout=0.2)
        return router

    async def test_log_success_does_not_ack_before_kafka_success(self):
        gate = asyncio.Event()
        kafka = RecordingDispatcher(gate)
        logger = RecordingDispatcher()
        router = self.make_router(kafka, logger)
        task = asyncio.create_task(router.publish(event()))
        await until(lambda: bool(logger.records))
        self.assertFalse(task.done())
        gate.set()
        await task
        self.assertEqual([kind for kind, _ in kafka.records], ["V"])
        self.assertEqual([kind for kind, _ in logger.records], ["V"])

    async def test_slow_logger_does_not_delay_kafka_success(self):
        gate = asyncio.Event()
        logger = RecordingDispatcher(gate)
        router = self.make_router(RecordingDispatcher(), logger)
        await asyncio.wait_for(router.publish(event()), timeout=0.2)
        await until(lambda: bool(logger.records))
        self.assertTrue(router.background_tasks)
        gate.set()

    async def test_failed_logger_does_not_fail_vehicle_delivery(self):
        router = self.make_router(RecordingDispatcher(), RecordingDispatcher(error=OSError("log failed")))
        await router.publish(event())
        await until(lambda: router.optional_failures.get(("V", "logger")) == 1)

    async def test_kafka_failure_is_not_hidden_by_successful_logger(self):
        logger = RecordingDispatcher()
        router = self.make_router(RecordingDispatcher(error=DeliveryError("offline")), logger)
        with self.assertRaises(DeliveryError):
            await router.publish(event())
        await until(lambda: bool(logger.records))

    async def test_background_limit_does_not_block_reliable_vehicle_delivery(self):
        gate = asyncio.Event()
        router = self.make_router(RecordingDispatcher(), RecordingDispatcher(gate), max_background_tasks=2)
        for number in range(5):
            router.notify("errors", {"event_id": str(number), "vehicle_id": "car"})
        self.assertEqual(len(router.background_tasks), 2)
        self.assertEqual(router.optional_dropped[("errors", "logger")], 3)
        await asyncio.wait_for(router.publish(event()), timeout=0.2)
        self.assertEqual(router.optional_dropped[("V", "logger")], 1)
        gate.set()

    async def test_destinations_receive_independent_copies(self):
        original = event()
        logger = RecordingDispatcher(mutate=True)
        kafka = RecordingDispatcher()
        router = self.make_router(kafka, logger)
        await router.publish(original)
        await until(lambda: bool(logger.records))
        self.assertEqual(original["signals"]["speed_mps"], 1.0)
        self.assertEqual(kafka.records[0][1]["signals"]["speed_mps"], 1.0)
        self.assertEqual(logger.records[0][1]["signals"]["speed_mps"], 999)

    async def test_close_cancels_optional_work_and_rejects_further_delivery(self):
        router = self.make_router(RecordingDispatcher(), RecordingDispatcher(asyncio.Event()))
        await router.publish(event())
        await until(lambda: bool(router.background_tasks))
        await router.close(timeout=0.01)
        self.assertFalse(router.background_tasks)
        with self.assertRaises(DeliveryError):
            await router.publish(event())

    async def test_record_type_is_part_of_kafka_deduplication_identity(self):
        native = FakeKafka()
        kafka = KafkaDispatcher(producer_factory=native.factory)
        vehicle = event()
        connectivity = {"event_id": vehicle["event_id"], "vehicle_id": vehicle["vehicle_id"],
                        "event_time": vehicle["event_time"], "status": "connected"}
        tasks = [
            asyncio.create_task(kafka.dispatch("V", vehicle)),
            asyncio.create_task(kafka.dispatch("connectivity", connectivity)),
        ]
        await asyncio.sleep(0)
        self.assertEqual(len(native.queue), 2)
        self.assertEqual({item["topic"] for item in native.queue},
                         {"vehicle-telemetry-v1", "vehicle-connectivity-v1"})
        native.poll()
        native.poll()
        await asyncio.gather(*tasks)
        self.assertEqual(kafka.pending, {})
        kafka.close()

    async def test_real_logger_console_write_runs_outside_event_loop_thread(self):
        lines = []
        threads = []
        def writer(line):
            lines.append(json.loads(line))
            threads.append(threading.get_ident())
        main_thread = threading.get_ident()
        await LoggerDispatcher(writer=writer).dispatch("V", event())
        self.assertEqual(lines[0]["record_type"], "V")
        self.assertEqual(lines[0]["signal_names"], ["location", "speed_mps"])
        self.assertNotIn("signals", lines[0])
        self.assertNotEqual(threads[0], main_thread)

    async def test_reception_generates_connection_and_validation_records_without_vehicle_ack(self):
        native = FakeKafka()
        kafka = KafkaDispatcher(producer_factory=native.factory)
        logger = RecordingDispatcher()
        router = DispatchRouter(
            {"kafka": kafka, "logger": logger},
            {"V": ["kafka", "logger"], "connectivity": ["logger"], "errors": ["logger"]},
            {"V": "kafka"},
        )
        self.addAsyncCleanup(router.close, timeout=0.2)
        class Socket:
            def __init__(self):
                self.sent = []
            async def recv(self):
                return json.dumps({"type": "hello", "vehicle_id": "car-1"})
            async def send(self, raw):
                self.sent.append(json.loads(raw))
            def __aiter__(self):
                return self
            async def __anext__(self):
                if len(self.sent) == 1:
                    return json.dumps({"type": "telemetry", "event": event(vehicle="wrong-car")})
                raise StopAsyncIteration
        socket = Socket()
        await FleetTelemetryServer(router).handle(socket)
        await until(lambda: len(logger.records) == 3)
        self.assertEqual([kind for kind, _ in logger.records], ["connectivity", "errors", "connectivity"])
        self.assertEqual(logger.records[0][1]["status"], "connected")
        self.assertEqual(logger.records[2][1]["status"], "disconnected")
        self.assertEqual(logger.records[0][1]["connection_id"], logger.records[2][1]["connection_id"])
        self.assertEqual([reply["type"] for reply in socket.sent], ["ready", "error"])
        self.assertFalse(native.attempts)
        kafka.close()

    async def test_default_routes_over_real_websocket_wait_for_kafka_but_not_logger(self):
        native = FakeKafka()
        native.hold = True
        topics = []
        original_produce = native.produce
        def record_topic(**message):
            topics.append(message["topic"])
            original_produce(**message)
        native.produce = record_topic
        with patch.dict(os.environ, {}, clear=True):
            args = parse_args([])
        kafka = KafkaDispatcher(producer_factory=native.factory, topics=args.kafka_topics)
        logger_gate = asyncio.Event()
        logger = RecordingDispatcher(logger_gate)
        router = build_dispatch_router(args, kafka_dispatcher=kafka, logger_dispatcher=logger,
                                       redis_dispatcher=RecordingDispatcher())
        self.addAsyncCleanup(router.close, timeout=0.2)
        self.addCleanup(kafka.close)
        async with serve(FleetTelemetryServer(router).handle, "127.0.0.1", 0) as server:
            url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
            async with connect(url, proxy=None) as client:
                await client.send(json.dumps({"type": "hello", "vehicle_id": "car-1"}))
                self.assertEqual(json.loads(await client.recv())["type"], "ready")
                await client.send(json.dumps({"type": "telemetry", "event": event()}))
                reply = asyncio.create_task(client.recv())
                await until(lambda: "vehicle-telemetry-v1" in topics)
                await asyncio.sleep(0.05)
                self.assertFalse(reply.done())
                native.hold = False
                while native.queue:
                    native.poll()
                ack = json.loads(await asyncio.wait_for(reply, timeout=1))
                self.assertEqual(ack["type"], "ack")
                self.assertFalse(logger_gate.is_set())
                await client.send(json.dumps({"type": "telemetry", "event": event(vehicle="wrong")}))
                self.assertEqual(json.loads(await client.recv())["type"], "error")
            await until(lambda: any(kind == "errors" for kind, _ in logger.records))
        while native.queue:
            native.poll()
        logger_gate.set()
        await router.close(timeout=1)
        self.assertEqual(set(topics), {
            "vehicle-telemetry-v1", "vehicle-connectivity-v1", "vehicle-errors-v1",
        })
        operational = [record for record in native.delivered if "record_type" in record]
        self.assertEqual({record["record_type"] for record in operational}, {"connectivity", "errors"})
        vehicle_records = [record for record in native.delivered if "record_type" not in record]
        self.assertEqual([record["event_id"] for record in vehicle_records], [event()["event_id"]])


class DispatchConfigurationTests(unittest.TestCase):
    def test_invalid_routes_and_ack_sources_fail_at_startup(self):
        original = json.loads(SERVER_TELEMETRY_CONFIG.read_text(encoding="utf-8"))
        changes = [
            {"records": {"V": ["logger"]}},
            {"records": {"V": ["kafka", "mqtt"]}},
            {"records": {"V": ["kafka", "kafka"]}},
            {"records": {"V": ["kafka"], "unknown": ["logger"]}},
            {"reliable_ack_sources": {"V": "logger"}},
            {"reliable_ack_sources": {"V": "kafka", "connectivity": "kafka"}},
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

    def test_record_topic_settings_are_overridden_with_role_named_environment(self):
        with patch.dict(os.environ, {
            "SERVER_KAFKA_CONNECTIVITY_TOPIC": "custom-connectivity",
            "SERVER_KAFKA_ERRORS_TOPIC": "custom-errors",
        }, clear=True):
            args = parse_args(["--kafka-vehicle-topic", "custom-vehicle"])
        self.assertEqual(args.kafka_topics, {
            "V": "custom-vehicle", "connectivity": "custom-connectivity", "errors": "custom-errors",
        })
        self.assertEqual(args.dispatch_records["V"], ["kafka", "logger", "redis"])

    def test_overriding_topics_cannot_mix_operational_records_with_vehicle_storage(self):
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                parse_args(["--kafka-connectivity-topic", "vehicle-telemetry-v1"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()

import asyncio
import json
import socket
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from server_kafka_dispatcher import KafkaDispatcher, DeliveryError
from server_dispatcher import DispatchRouter
from server_fleet_telemetry import FleetTelemetryServer, poll_kafka
from vehicle_fleet_telemetry_client import VehicleFleetTelemetryClient, SimulatedFleetTelemetryClients
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


def event(number=1, vehicle="car-1", signals=None):
    return {
        "event_id": f"run:{vehicle}:{number}", "vehicle_id": vehicle,
        "run_id": "run", "sequence_no": number,
        "event_time": f"2026-10-06T00:00:{number:02d}.000Z",
        "signals": {"location": {"latitude": 37.0 + number / 100, "longitude": 127.0},
                    "speed_mps": float(number)} if signals is None else signals,
    }


async def until(predicate, timeout=3):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Timed out waiting for test state")
        await asyncio.sleep(0.01)


class FakeKafka:
    def __init__(self):
        self.online = True
        self.hold = False
        self.queue_full = False
        self.queue = []
        self.attempts = []
        self.delivered = []
        self.configuration = None

    def factory(self, configuration):
        self.configuration = configuration
        return self

    def produce(self, **message):
        if self.queue_full:
            raise BufferError("test queue full")
        self.attempts.append(json.loads(message["value"]))
        self.queue.append(message)

    def poll(self, timeout=0):
        if not self.queue or self.hold:
            return
        message = self.queue.pop(0)
        if self.online:
            self.delivered.append(json.loads(message["value"]))
            message["on_delivery"](None, None)
        else:
            message["on_delivery"]("test Kafka offline", None)

    def flush(self, timeout=0):
        if not self.hold:
            while self.queue:
                self.poll()
        return len(self.queue)


class KafkaDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_waits_for_kafka_success(self):
        kafka = FakeKafka()
        sink = KafkaDispatcher(producer_factory=kafka.factory)
        task = asyncio.create_task(sink.publish(event()))
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertEqual(kafka.configuration["acks"], "all")
        self.assertTrue(kafka.configuration["enable.idempotence"])
        kafka.poll()
        await task
        self.assertEqual(sink.pending, {})
        self.assertIn("received_at", kafka.delivered[0])

    async def test_concurrent_duplicate_uses_one_native_publish(self):
        kafka = FakeKafka()
        sink = KafkaDispatcher(producer_factory=kafka.factory)
        tasks = [asyncio.create_task(sink.publish(event())) for _ in range(2)]
        await asyncio.sleep(0)
        self.assertEqual(len(kafka.attempts), 1)
        kafka.poll()
        await asyncio.gather(*tasks)

    async def test_conflicting_id_rejected(self):
        kafka = FakeKafka()
        sink = KafkaDispatcher(producer_factory=kafka.factory)
        task = asyncio.create_task(sink.publish(event()))
        await asyncio.sleep(0)
        changed = event()
        changed["signals"]["speed_mps"] = 50
        with self.assertRaises(DeliveryError):
            await sink.publish(changed)
        kafka.poll()
        await task

    async def test_queue_full_and_final_failure_return_error(self):
        for queue_full in (True, False):
            kafka = FakeKafka()
            kafka.queue_full = queue_full
            kafka.online = False
            sink = KafkaDispatcher(producer_factory=kafka.factory)
            task = asyncio.create_task(sink.publish(event()))
            await asyncio.sleep(0)
            kafka.poll()
            with self.assertRaises(DeliveryError):
                await task
            self.assertEqual(sink.pending, {})

    async def test_cancelled_socket_wait_does_not_cancel_kafka_publish(self):
        kafka = FakeKafka()
        sink = KafkaDispatcher(producer_factory=kafka.factory)
        task = asyncio.create_task(sink.publish(event()))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(sink.pending), 1)
        kafka.poll()
        self.assertEqual(len(kafka.delivered), 1)
        self.assertEqual(sink.pending, {})

    async def test_real_native_client_timeout_returns_failure(self):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
            sink = KafkaDispatcher(bootstrap_servers=f"127.0.0.1:{port}", message_timeout_ms=1000)
            stop = asyncio.Event()
            polling = asyncio.create_task(poll_kafka(sink, stop))
            try:
                with self.assertRaises(DeliveryError):
                    await asyncio.wait_for(sink.publish(event()), timeout=5)
                self.assertEqual(sink.pending, {})
            finally:
                stop.set()
                await polling
                sink.close()


class WebSocketPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="telemetry-ws-test-")
        self.buffer = VehicleSQLiteBuffer(Path(self.directory.name) / "vehicle.db")
        self.kafka = FakeKafka()
        self.sink = KafkaDispatcher(producer_factory=self.kafka.factory)
        self.router = DispatchRouter(
            {"kafka": self.sink}, {"V": ["kafka"]}, {"V": "kafka"},
        )
        self.stop_polling = asyncio.Event()
        self.polling = asyncio.create_task(poll_kafka(self.sink, self.stop_polling))
        self.server = await serve(FleetTelemetryServer(self.router).handle, "127.0.0.1", 0)
        self.url = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        self.clients = []

    async def asyncTearDown(self):
        for stop, task in self.clients:
            stop.set()
            task.cancel()
        await asyncio.gather(*(task for _, task in self.clients), return_exceptions=True)
        self.server.close()
        await self.server.wait_closed()
        await self.router.close()
        self.stop_polling.set()
        await self.polling
        self.sink.close()
        self.buffer.close()
        self.directory.cleanup()

    def start_sender(self, **settings):
        settings.setdefault("max_in_flight", 4)
        settings.setdefault("ack_timeout", 1.0)
        sender = VehicleFleetTelemetryClient(
            self.buffer, "car-1", self.url, retry_seconds=0.01,
            retry_max_seconds=0.05, **settings,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(sender.run(stop))
        self.clients.append((stop, task))
        return sender

    async def test_sqlite_survives_enqueue_and_deletes_only_after_ack(self):
        self.buffer.save(event())
        self.kafka.hold = True
        sender = self.start_sender()
        await until(lambda: len(self.kafka.attempts) == 1)
        await asyncio.sleep(0.1)
        self.assertEqual(len(self.buffer.get_pending()), 1)
        self.assertEqual(len(self.kafka.attempts), 1)
        self.kafka.hold = False
        await until(lambda: not self.buffer.get_pending())
        self.assertEqual(sender.delivered_count, 1)

    async def test_outage_collection_then_latest_and_all_history_delivered(self):
        self.buffer.save(event(1))
        self.kafka.online = False
        sender = self.start_sender(max_in_flight=1)
        # 실제 실패 응답이 차량에 도착한 뒤 복구합니다. 이미 접수된 기록은 취소할 수 없습니다.
        await until(lambda: sender.backoff.attempt > 0)
        for number in range(2, 7):
            self.buffer.save(event(number))
        self.assertEqual(len(self.buffer.get_pending()), 6)
        self.kafka.online = True
        await until(lambda: not self.buffer.get_pending())
        self.assertEqual(self.kafka.delivered[0]["sequence_no"], 6)
        self.assertCountEqual([e["sequence_no"] for e in self.kafka.delivered], range(1, 7))
        from server_telemetry_store import ServerTelemetryStore
        store = ServerTelemetryStore(Path(self.directory.name) / "server.db")
        try:
            for record in self.kafka.delivered:
                store.save(record)
            self.assertEqual(store.connection.execute("SELECT COUNT(*) FROM telemetry_history").fetchone()[0], 6)
            latest = dict(store.connection.execute("SELECT signal, value FROM vehicle_latest_state"))
            self.assertEqual(json.loads(latest["location"]), event(6)["signals"]["location"])
            self.assertEqual(json.loads(latest["speed_mps"]), 6.0)
        finally:
            store.close()

    async def test_newest_per_signal_is_prioritized(self):
        self.buffer.save(event(1))
        self.buffer.save(event(2, signals={"location": {"latitude": 37.2, "longitude": 127.2}}))
        self.buffer.save(event(3, signals={"speed_mps": 20.0}))
        self.start_sender()
        await until(lambda: not self.buffer.get_pending())
        self.assertEqual([e["sequence_no"] for e in self.kafka.delivered], [3, 2, 1])

    async def test_each_vehicle_has_its_own_connection_and_buffer_records(self):
        for car in ("car-1", "car-2"):
            self.buffer.save(event(1, vehicle=car))
        stop = asyncio.Event()
        task = asyncio.create_task(SimulatedFleetTelemetryClients(self.buffer, fleet_telemetry_server_url=self.url).run(stop))
        self.clients.append((stop, task))
        await until(lambda: not self.buffer.get_pending())
        self.assertCountEqual([e["vehicle_id"] for e in self.kafka.delivered], ["car-1", "car-2"])

    async def test_queue_limit_and_no_duplicate_while_waiting(self):
        for number in range(1, 7):
            self.buffer.save(event(number))
        self.kafka.hold = True
        sender = self.start_sender(max_in_flight=2)
        await until(lambda: len(self.kafka.attempts) == 2)
        await asyncio.sleep(0.1)
        self.assertEqual(len(sender.in_flight), 2)
        self.assertEqual(len(self.kafka.attempts), 2)
        self.assertEqual(len(self.buffer.get_pending()), 6)
        self.kafka.hold = False
        await until(lambda: not self.buffer.get_pending())

    async def test_local_delete_failure_retries_without_network_resend(self):
        self.buffer.save(event())
        self.buffer.connection.execute("""
            CREATE TRIGGER fail_delete BEFORE DELETE ON events
            BEGIN SELECT RAISE(ABORT, 'test local delete failure'); END
        """)
        sender = self.start_sender()
        await until(lambda: bool(sender.pending_acks))
        self.assertEqual(len(self.kafka.delivered), 1)
        self.assertEqual(len(self.buffer.get_pending()), 1)
        self.buffer.connection.execute("DROP TRIGGER fail_delete")
        await until(lambda: not self.buffer.get_pending())
        self.assertEqual(len(self.kafka.attempts), 1)

    async def test_ack_timeout_retains_record_and_replays_original_id(self):
        self.buffer.save(event())
        self.kafka.hold = True
        sender = self.start_sender(max_in_flight=1, ack_timeout=0.1)
        await until(lambda: sender.backoff.attempt > 0)
        self.assertEqual(len(self.buffer.get_pending()), 1)
        self.assertEqual(len(self.kafka.attempts), 1)
        sender.ack_timeout = 1.0
        self.kafka.hold = False
        await until(lambda: not self.buffer.get_pending())
        # Kafka 성공 직후 재접수되면 중복은 허용됩니다. 모든 전송은 같은 원본 ID입니다.
        self.assertTrue(self.kafka.delivered)
        self.assertEqual({e["event_id"] for e in self.kafka.delivered}, {event()["event_id"]})

    async def test_receiver_disconnect_keeps_records_for_reconnect(self):
        self.buffer.save(event())
        self.kafka.hold = True
        sender = self.start_sender()
        await until(lambda: len(self.kafka.attempts) == 1)
        self.server.close()
        await self.server.wait_closed()
        await until(lambda: sender.backoff.attempt > 0)
        self.buffer.save(event(2))
        self.buffer.save(event(3))
        self.assertEqual(len(self.buffer.get_pending()), 3)
        self.server = await serve(FleetTelemetryServer(self.router).handle, "127.0.0.1", 0)
        sender.fleet_telemetry_server_url = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        self.kafka.hold = False
        await until(lambda: not self.buffer.get_pending(), timeout=5)
        self.assertEqual({e["sequence_no"] for e in self.kafka.delivered}, {1, 2, 3})

    async def test_history_has_turn_even_when_new_positions_keep_arriving(self):
        self.buffer.save(event(1))
        sender = VehicleFleetTelemetryClient(self.buffer, "car-1", max_in_flight=1)
        sent = []
        class LocalSocket:
            async def send(self, raw):
                sent.append(json.loads(raw)["event"])
        for number in (2, 3, 4, 5):
            self.buffer.save(event(number))
            await sender._enqueue_available(LocalSocket())
            sender._accept_ack({
                "type": "ack", "vehicle_id": "car-1", "event_id": sent[-1]["event_id"],
            })
        self.assertEqual([e["sequence_no"] for e in sent], [2, 3, 4, 1])
        self.assertEqual([e["sequence_no"] for e in self.buffer.get_pending()], [5])

    async def test_connection_identity_mismatch_rejected_without_publish(self):
        async with connect(self.url, proxy=None) as client:
            await client.send(json.dumps({"type": "hello", "vehicle_id": "car-1"}))
            await client.recv()
            await client.send(json.dumps({"type": "telemetry", "event": event(vehicle="car-2")}))
            response = json.loads(await client.recv())
            self.assertEqual(response["type"], "error")
        self.assertEqual(self.kafka.attempts, [])

    async def test_lost_ack_preserves_record_and_retransmits_same_id(self):
        self.server.close()
        await self.server.wait_closed()
        regular = FleetTelemetryServer(self.router)
        ack_lost = asyncio.Event()
        allow_close = asyncio.Event()
        first = True

        async def drop_first_ack(client):
            nonlocal first
            if not first:
                await regular.handle(client)
                return
            first = False
            hello = json.loads(await client.recv())
            await client.send(json.dumps({"type": "ready", "vehicle_id": hello["vehicle_id"]}))
            request = json.loads(await client.recv())
            await self.sink.publish(request["event"])
            ack_lost.set()
            await allow_close.wait()
            await client.close(code=1012, reason="test ACK loss")

        self.server = await serve(drop_first_ack, "127.0.0.1", 0)
        self.url = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        self.buffer.save(event())
        self.start_sender()
        await asyncio.wait_for(ack_lost.wait(), timeout=2)
        self.assertEqual(len(self.buffer.get_pending()), 1)
        allow_close.set()
        await until(lambda: not self.buffer.get_pending())
        self.assertEqual([e["event_id"] for e in self.kafka.delivered], [event()["event_id"]] * 2)


if __name__ == "__main__":
    unittest.main()

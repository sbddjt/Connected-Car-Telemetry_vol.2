"""차량 역할: SQLite 기록을 WebSocket으로 보내고 서버 ACK 후 삭제합니다."""
import argparse
import asyncio
import json
import sqlite3
import ssl
import time
from shared_pipeline_metrics import PipelineMetrics
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from vehicle_sharded_buffer import shard_path
from shared_fleet_telemetry_policy import (
    Backoff, BUFFER_MESSAGES_PER_VEHICLE, LIVE_WEIGHT, MAX_IN_FLIGHT_PER_VEHICLE, signals_of,
)
from shared_runtime_config import (
    VEHICLE_TELEMETRY_CONFIG, add_env_argument, load_vehicle_telemetry_config, project_path,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def pause(stop: asyncio.Event, seconds: float):
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


class VehicleFleetTelemetryClient:
    def __init__(
        self, buffer: VehicleSQLiteBuffer, vehicle_id: str, fleet_telemetry_server_url="ws://127.0.0.1:8765",
        max_in_flight=MAX_IN_FLIGHT_PER_VEHICLE, live_weight=LIVE_WEIGHT,
        ack_timeout=45.0, retry_seconds=1.0, retry_max_seconds=30.0, tls=None, defer_local_deletes=False,
    ):
        if max_in_flight < 1 or live_weight < 1 or not 0 < ack_timeout < float("inf"):
            raise ValueError("Require positive finite delivery settings")
        self.buffer = buffer
        self.vehicle_id = vehicle_id
        self.fleet_telemetry_server_url = fleet_telemetry_server_url
        self.max_in_flight = max_in_flight
        self.live_weight = live_weight
        self.ack_timeout = ack_timeout
        self.tls = tls
        self.defer_local_deletes = defer_local_deletes
        self.backoff = Backoff(retry_seconds, retry_max_seconds)
        self.in_flight = {}
        self.pending_acks: dict[str, Backoff] = {}
        self.latest_acknowledged = {}
        self.lane_turn = 0
        self.delivered_count = 0

    def _delete_acknowledged(self, event_id: str):
        try:
            self.buffer.acknowledge(event_id)
        except sqlite3.Error as error:
            retry = self.pending_acks[event_id]
            retry.fail(time.monotonic())
            print(f"[로컬 삭제 재시도] event_id={event_id} | error={error}")
        else:
            del self.pending_acks[event_id]

    def _retry_local_deletes(self):
        if self.defer_local_deletes:
            return
        now = time.monotonic()
        for event_id, retry in list(self.pending_acks.items()):
            if now >= retry.after:
                self._delete_acknowledged(event_id)

    def _accept_ack(self, reply: dict):
        if reply.get("vehicle_id") != self.vehicle_id:
            raise ValueError("ACK vehicle_id mismatch")
        event_id = reply.get("event_id")
        pending = self.in_flight.pop(event_id, None)
        if pending is None:
            return  # 수신 확인한 이벤트 이외의 SQLite 행은 삭제하지 않습니다.
        row_id, event, _ = pending
        self.backoff.reset()
        for signal in signals_of(event) or {"snapshot": None}:
            key = (self.vehicle_id, signal)
            self.latest_acknowledged[key] = max(row_id, self.latest_acknowledged.get(key, 0))
        self.delivered_count += 1
        self.pending_acks[event_id] = Backoff(self.backoff.initial, self.backoff.maximum)
        if not self.defer_local_deletes:
            self._delete_acknowledged(event_id)

    async def _enqueue_available(self, socket):
        blocked = set(self.in_flight) | self.pending_acks.keys()
        available = self.max_in_flight - len(blocked)
        if available < 1:
            return
        live = self.buffer.get_latest_pending(
            limit=available, exclude_ids=blocked, vehicle_id=self.vehicle_id,
            acknowledged_rows=self.latest_acknowledged,
        )
        history = self.buffer.get_pending_records(
            limit=available, exclude_ids=blocked, vehicle_id=self.vehicle_id,
        )
        for _ in range(available):
            while live and live[0][1]["event_id"] in blocked:
                live.pop(0)
            while history and history[0][1]["event_id"] in blocked:
                history.pop(0)
            if not live and not history:
                break
            prefer_live = self.lane_turn < self.live_weight
            lane = live if live and (prefer_live or not history) else history
            row_id, event = lane.pop(0)
            event_id = event["event_id"]
            self.in_flight[event_id] = (row_id, event, time.monotonic())
            await socket.send(json.dumps({"type": "telemetry", "event": event}, allow_nan=False))
            blocked.add(event_id)
            self.lane_turn = (self.lane_turn + 1) % (self.live_weight + 1)

    async def _session(self, socket, stop):
        receiving = asyncio.create_task(socket.recv())
        stopping = asyncio.create_task(stop.wait())
        idle_since = time.monotonic()
        try:
            while not stop.is_set():
                if receiving.done():
                    reply = json.loads(receiving.result())
                    if not isinstance(reply, dict):
                        raise ValueError("Invalid server reply")
                    if reply.get("type") == "ack":
                        self._accept_ack(reply)
                    else:
                        raise ValueError(f"Server rejected record: {reply.get('error', reply)}")
                    receiving = asyncio.create_task(socket.recv())
                self._retry_local_deletes()
                await self._enqueue_available(socket)
                if self.in_flight or self.pending_acks:
                    idle_since = time.monotonic()
                elif time.monotonic() - idle_since >= 5.0:
                    return  # 기록이 없는 차량의 연결은 정리하고 다음 기록 때 다시 만듭니다.
                if any(time.monotonic() - sent >= self.ack_timeout for _, _, sent in self.in_flight.values()):
                    raise TimeoutError("Server ACK timeout")
                await asyncio.wait((receiving, stopping), timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        finally:
            receiving.cancel()
            stopping.cancel()
            await asyncio.gather(receiving, stopping, return_exceptions=True)
            # ACK를 못 받은 기록은 SQLite에 남아 있습니다. 재연결 후 같은 ID로 재전송합니다.
            self.in_flight.clear()

    async def run(self, stop: asyncio.Event):
        idle_since = time.monotonic()
        while not stop.is_set():
            try:
                self._retry_local_deletes()
                if not self.buffer.get_pending_records(
                    limit=1, exclude_ids=set(self.pending_acks), vehicle_id=self.vehicle_id,
                ):
                    if time.monotonic() - idle_since >= 5 and not self.pending_acks:
                        return  # 이전 차량의 유휴 작업을 누적시키지 않고 신규 기록 때 다시 생성합니다.
                    await pause(stop, 0.2)
                    continue
                idle_since = time.monotonic()
                kwargs = {"proxy": None, "open_timeout": 5, "close_timeout": 1, "max_size": 1_000_000}
                if self.tls is not None:
                    kwargs["ssl"] = self.tls
                async with connect(self.fleet_telemetry_server_url, **kwargs) as socket:
                    await socket.send(json.dumps({"type": "hello", "vehicle_id": self.vehicle_id}))
                    reply = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
                    if reply != {"type": "ready", "vehicle_id": self.vehicle_id}:
                        raise ValueError("Invalid server hello")
                    await self._session(socket, stop)
                    return
            except (OSError, WebSocketException, TimeoutError, ValueError, sqlite3.Error) as error:
                self.in_flight.clear()
                delay = self.backoff.fail(time.monotonic())
                print(f"[차량 재연결] vehicle={self.vehicle_id} | {delay:.2f}초 후 | error={error}")
                await pause(stop, delay)


class SimulatedFleetTelemetryClients:
    """한 SUMO 실험의 차량들을 차량별 WebSocket 연결로 모방합니다."""
    def __init__(self, buffer: VehicleSQLiteBuffer, max_workers=128, **sender_options):
        self.buffer = buffer
        self.sender_options = sender_options
        self.max_workers = max_workers
        self.workers = {}
        self.senders = {}
        self.retired_deliveries = 0
        self.metrics = PipelineMetrics("vehicle-sender")

    def _flush_acknowledged(self):
        now=time.monotonic()
        ready=[(sender,event_id) for sender in self.senders.values()
               for event_id,retry in sender.pending_acks.items() if now>=retry.after]
        if not ready:
            return
        try:
            self.buffer.acknowledge_many([event_id for sender,event_id in ready])
        except sqlite3.Error as error:
            for sender,event_id in ready:
                sender.pending_acks[event_id].fail(now)
            print(f"[버퍼 ACK 삭제 묶음 재시도] {error}")
        else:
            for sender,event_id in ready:
                del sender.pending_acks[event_id]

    async def run(self, stop: asyncio.Event):
        try:
            while not stop.is_set():
                try:
                    vehicle_ids = self.buffer.pending_vehicles()
                except sqlite3.Error as error:
                    print(f"[버퍼 조회 재시도] {error}")
                    await pause(stop, 1.0)
                    continue
                for vehicle_id, task in list(self.workers.items()):
                    if task.done():
                        task.result()  # 프로그래밍 오류를 조용히 무시하지 않습니다.
                        del self.workers[vehicle_id]
                        self.retired_deliveries += self.senders.pop(vehicle_id).delivered_count
                for vehicle_id in vehicle_ids:
                    if vehicle_id not in self.workers:
                        if len(self.workers) >= self.max_workers:
                            break
                        sender = VehicleFleetTelemetryClient(self.buffer, vehicle_id, defer_local_deletes=True, **self.sender_options)
                        self.senders[vehicle_id] = sender
                        self.workers[vehicle_id] = asyncio.create_task(sender.run(stop))
                self._flush_acknowledged()
                if time.monotonic() >= self.metrics.after:
                    pending = self.buffer.connection.execute("SELECT COUNT(*) FROM events WHERE delivered=0").fetchone()[0]
                    dropped = self.buffer.connection.execute("SELECT value FROM buffer_stats WHERE name='capacity_dropped'").fetchone()[0]
                    delivered = self.retired_deliveries + sum(sender.delivered_count for sender in self.senders.values())
                    self.metrics.publish(delivered, pending_events=pending, capacity_dropped=dropped,
                                         active_workers=len(self.workers),
                                         in_flight=sum(len(sender.in_flight) for sender in self.senders.values()))
                await pause(stop, 0.2)
        finally:
            for task in self.workers.values():
                task.cancel()
            await asyncio.gather(*self.workers.values(), return_exceptions=True)


async def run_sender(args):
    tls = None
    if args.ca_file or args.certfile or args.keyfile:
        if not args.fleet_telemetry_server_url.startswith("wss://") or not all((args.ca_file, args.certfile, args.keyfile)):
            raise ValueError("mTLS requires wss://, --vehicle-tls-ca-file, --vehicle-tls-client-cert and --vehicle-tls-client-key")
        tls = ssl.create_default_context(cafile=args.ca_file)
        tls.load_cert_chain(args.certfile, args.keyfile)
    buffer_path = (shard_path(args.vehicle_buffer_directory, args.vehicle_buffer_shard_index, args.vehicle_buffer_shards)
                   if args.vehicle_buffer_directory else args.vehicle_buffer_db)
    buffer = VehicleSQLiteBuffer(buffer_path, max_events=args.vehicle_buffer_max_messages)
    try:
        buffer.cleanup_completed()
        print(f"[차량 전송 시작] db={buffer_path} | server={args.fleet_telemetry_server_url}")
        await SimulatedFleetTelemetryClients(
            buffer, max_workers=args.max_workers, fleet_telemetry_server_url=args.fleet_telemetry_server_url, max_in_flight=args.max_in_flight,
            live_weight=args.live_weight, ack_timeout=args.ack_timeout,
            retry_seconds=args.retry_seconds, retry_max_seconds=args.retry_max_seconds, tls=tls,
        ).run(asyncio.Event())
    finally:
        buffer.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="차량 SQLite 버퍼 → Fleet Telemetry 서버")
    add_env_argument(parser, "--vehicle-telemetry-config", env="VEHICLE_FLEET_TELEMETRY_CONFIG_PATH",
                     default=VEHICLE_TELEMETRY_CONFIG, type=project_path)
    add_env_argument(parser, "--vehicle-buffer-db", aliases=("--db-path",), dest="vehicle_buffer_db",
                     env="VEHICLE_SQLITE_BUFFER_DB_PATH",
                     default=PROJECT_ROOT / "data" / "event_buffer.db", type=project_path)
    add_env_argument(parser, "--vehicle-buffer-max-messages", env="VEHICLE_BUFFER_MAX_MESSAGES",
                     default=BUFFER_MESSAGES_PER_VEHICLE, type=int)
    add_env_argument(parser, "--fleet-telemetry-server-url", aliases=("--server-url",), dest="fleet_telemetry_server_url",
                     env="VEHICLE_FLEET_TELEMETRY_SERVER_URL", default="ws://127.0.0.1:8765")
    add_env_argument(parser, "--vehicle-max-in-flight", aliases=("--max-in-flight",), dest="max_in_flight",
                     env="VEHICLE_MAX_IN_FLIGHT_MESSAGES", default=MAX_IN_FLIGHT_PER_VEHICLE, type=int)
    add_env_argument(parser, "--vehicle-live-weight", aliases=("--live-weight",), dest="live_weight",
                     env="VEHICLE_LIVE_SEND_WEIGHT", default=LIVE_WEIGHT, type=int)
    add_env_argument(parser, "--vehicle-ack-timeout-seconds", aliases=("--ack-timeout",), dest="ack_timeout",
                     env="VEHICLE_ACK_TIMEOUT_SECONDS", default=45.0, type=float)
    add_env_argument(parser, "--vehicle-reconnect-initial-seconds", aliases=("--retry-seconds",),
                     dest="retry_seconds", env="VEHICLE_RECONNECT_INITIAL_SECONDS", default=1.0, type=float)
    add_env_argument(parser, "--vehicle-reconnect-max-seconds", aliases=("--retry-max-seconds",),
                     dest="retry_max_seconds", env="VEHICLE_RECONNECT_MAX_SECONDS", default=30.0, type=float)
    add_env_argument(parser, "--vehicle-tls-ca-file", aliases=("--ca-file",), dest="ca_file",
                     env="VEHICLE_TLS_CA_FILE", type=project_path)
    add_env_argument(parser, "--vehicle-tls-client-cert", aliases=("--certfile",), dest="certfile",
                     env="VEHICLE_TLS_CLIENT_CERT_FILE", type=project_path)
    add_env_argument(parser, "--vehicle-tls-client-key", aliases=("--keyfile",), dest="keyfile",
                     env="VEHICLE_TLS_CLIENT_KEY_FILE", type=project_path)
    add_env_argument(parser, "--vehicle-buffer-directory", env="VEHICLE_SQLITE_BUFFER_DIRECTORY", default=None, type=lambda value: project_path(value) if value else None)
    add_env_argument(parser, "--vehicle-buffer-shards", env="VEHICLE_BUFFER_SHARDS", default=8, type=int)
    add_env_argument(parser, "--vehicle-buffer-shard-index", env="VEHICLE_BUFFER_SHARD_INDEX", default=0, type=int)
    add_env_argument(parser, "--vehicle-sender-max-workers", env="VEHICLE_SENDER_MAX_WORKERS", default=128, type=int, dest="max_workers")
    args = parser.parse_args(argv)
    if args.max_workers < 1:
        parser.error("Require positive vehicle-sender-max-workers")
    if not 1 <= args.vehicle_buffer_shards <= 64 or not 0 <= args.vehicle_buffer_shard_index < args.vehicle_buffer_shards:
        parser.error("Require valid vehicle buffer shard count and index")
    try:
        load_vehicle_telemetry_config(args.vehicle_telemetry_config)
        Backoff(args.retry_seconds, args.retry_max_seconds)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    # 잘못된 옵션은 아직 이벤트가 없는 실행에서도 즉시 알려줍니다.
    if args.vehicle_buffer_max_messages < 1 or args.max_in_flight < 1 or args.live_weight < 1 or not 0 < args.ack_timeout < float("inf"):
        parser.error("Require positive finite delivery settings")
    return args


def main():
    args = parse_args()
    try:
        asyncio.run(run_sender(args))
    except KeyboardInterrupt:
        print("[차량 전송 종료] 미확인 이벤트는 SQLite에 보관합니다.")


if __name__ == "__main__":
    main()

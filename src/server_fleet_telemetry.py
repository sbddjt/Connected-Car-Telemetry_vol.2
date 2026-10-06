"""WebSocket으로 차량 기록을 받고 Kafka ACK 이후 성공 확인을 돌려줍니다."""
import argparse
import asyncio
import contextlib
import json
import ssl
from datetime import datetime, timezone
from uuid import uuid4

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from server_dispatcher import DeliveryError, DispatchRouter
from datastore.kafka.kafka import Producer as KafkaProducer
from datastore.simple.logger import Producer as LoggerProducer
from datastore.redis.redis import Producer as RedisProducer
from shared_redis_config import add_redis_arguments, finish_redis_arguments
from shared_fleet_telemetry_policy import validate_event
from shared_runtime_config import add_env_argument, project_path, server_config_argument


class FleetTelemetryServer:
    def __init__(self, dispatcher: DispatchRouter, max_per_connection: int = 100):
        if max_per_connection < 1:
            raise ValueError("max_per_connection must be positive")
        self.dispatcher = dispatcher
        self.max_per_connection = max_per_connection

    def _notify(self, record_type, vehicle_id, connection_id, **fields):
        self.dispatcher.notify(record_type, {
            "event_id": str(uuid4()), "vehicle_id": vehicle_id,
            "event_time": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "connection_id": connection_id, **fields,
        })

    async def handle(self, socket):
        tasks = set()
        vehicle_id = None
        connection_id = str(uuid4())
        connected = False
        try:
            hello = json.loads(await asyncio.wait_for(socket.recv(), timeout=5))
            if (not isinstance(hello, dict) or hello.get("type") != "hello"
                    or not isinstance(hello.get("vehicle_id"), str)
                    or not 1 <= len(hello["vehicle_id"]) <= 512):
                await socket.close(code=1008, reason="Invalid vehicle hello")
                return
            vehicle_id = hello["vehicle_id"]
            await socket.send(json.dumps({"type": "ready", "vehicle_id": vehicle_id}))
            connected = True
            self._notify("connectivity", vehicle_id, connection_id, status="connected")
            async for raw in socket:
                event_id = None
                try:
                    envelope = json.loads(raw)
                    if not isinstance(envelope, dict) or envelope.get("type") != "telemetry":
                        raise ValueError("Expected telemetry message")
                    event = validate_event(envelope.get("event"))
                    event_id = event["event_id"]
                    if event["vehicle_id"] != vehicle_id:
                        raise ValueError("vehicle_id does not match connection")
                except (ValueError, TypeError, KeyError) as error:
                    self._notify("errors", vehicle_id, connection_id, stage="validation",
                                 source_event_id=event_id, error=str(error))
                    await socket.send(json.dumps({
                        "type": "error", "event_id": event_id, "retryable": False,
                        "error": str(error),
                    }))
                    continue
                if len(tasks) >= self.max_per_connection:
                    self._notify("errors", vehicle_id, connection_id, stage="backpressure",
                                 source_event_id=event_id, error="Connection queue is full")
                    # 한도 초과 기록은 성공 확인 없이 차량 버퍼에 남깁니다.
                    await socket.send(json.dumps({
                        "type": "error", "event_id": event_id, "retryable": True,
                        "error": "Connection queue is full",
                    }))
                    continue
                task = asyncio.create_task(self._deliver(socket, event, connection_id))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        except (ConnectionClosed, TimeoutError, ValueError, TypeError):
            pass
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if connected:
                self._notify("connectivity", vehicle_id, connection_id, status="disconnected")

    async def _deliver(self, socket, event, connection_id=None):
        try:
            try:
                await self.dispatcher.dispatch("V", event)
            except DeliveryError as error:
                self._notify("errors", event["vehicle_id"], connection_id, stage="kafka_delivery",
                             source_event_id=event["event_id"], error=str(error))
                reply = {
                    "type": "error", "event_id": event["event_id"],
                    "retryable": True, "error": str(error),
                }
            else:
                # 접수 직후에는 ACK하지 않습니다. Kafka 최종 성공 후에만 보냅니다.
                reply = {
                    "type": "ack", "event_id": event["event_id"],
                    "vehicle_id": event["vehicle_id"],
                }
            await socket.send(json.dumps(reply))
        except ConnectionClosed:
            # Kafka에 저장됐어도 ACK가 유실될 수 있습니다. 재접수 중복은 Consumer가 처리합니다.
            pass


async def poll_kafka(dispatcher, stop):
    while not stop.is_set():
        dispatcher.poll()
        await asyncio.sleep(0.01)


def build_dispatch_router(args, *, kafka_dispatcher=None, logger_dispatcher=None, redis_dispatcher=None):
    kafka = kafka_dispatcher if kafka_dispatcher is not None else KafkaProducer(
        args.kafka_bootstrap_servers, args.kafka_vehicle_topic, args.kafka_message_timeout_ms,
        topics=args.kafka_topics,
    )
    dispatchers = {"kafka": kafka}
    if any("logger" in targets for targets in args.dispatch_records.values()):
        dispatchers["logger"] = logger_dispatcher if logger_dispatcher is not None else LoggerProducer(
            verbose=args.dispatch_logger_verbose,
        )
    if any("redis" in targets for targets in args.dispatch_records.values()):
        dispatchers["redis"] = redis_dispatcher if redis_dispatcher is not None else RedisProducer(
            args.server_redis_url, args.redis_namespace, args.redis_publish_vin_topics,
            args.redis_subscriber_set_prefix, args.redis_publish_timeout / 1_000_000_000,
            client_options=args.redis_client_options,
        )
    return DispatchRouter(dispatchers, args.dispatch_records, args.reliable_ack_sources)


async def run_server(args):
    tls = None
    if args.certfile or args.keyfile or args.client_ca:
        if not all((args.certfile, args.keyfile, args.client_ca)):
            raise ValueError("TLS requires --server-tls-cert, --server-tls-key and --server-tls-client-ca")
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(args.certfile, args.keyfile)
        tls.load_verify_locations(args.client_ca)
        tls.verify_mode = ssl.CERT_REQUIRED
    router = build_dispatch_router(args)
    stop = asyncio.Event()
    polling = asyncio.create_task(poll_kafka(router, stop))
    try:
        async with serve(FleetTelemetryServer(router).handle, args.server_host, args.server_port, ssl=tls, max_size=1_000_000):
            print(f"[수신 서버] {'wss' if tls else 'ws'}://{args.server_host}:{args.server_port}")
            await asyncio.Event().wait()
    finally:
        await router.close()
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await polling
        if "redis" in router.dispatchers:
            await router.dispatchers["redis"].close()
        router.dispatchers["kafka"].close()
        print(f"[Dispatcher 종료] 선택 목적지 실패={router.optional_failures} | "
              f"한도 초과 접수 제외={router.optional_dropped}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Fleet Telemetry 차량 수신 및 Kafka ACK")
    config = server_config_argument(parser, argv)
    add_env_argument(parser, "--server-host", aliases=("--host",), dest="server_host",
                     env="SERVER_FLEET_TELEMETRY_HOST", default=config["host"])
    add_env_argument(parser, "--server-port", aliases=("--port",), dest="server_port",
                     env="SERVER_FLEET_TELEMETRY_PORT", default=config["port"], type=int)
    add_env_argument(parser, "--kafka-bootstrap-servers", aliases=("--bootstrap-servers",),
                     dest="kafka_bootstrap_servers", env="SERVER_KAFKA_BOOTSTRAP_SERVERS",
                     default=config["kafka"]["bootstrap.servers"])
    add_env_argument(parser, "--kafka-vehicle-topic", aliases=("--topic",), dest="kafka_vehicle_topic",
                     env="SERVER_KAFKA_VEHICLE_TOPIC", default=config["kafka_topics"]["V"])
    add_env_argument(parser, "--kafka-connectivity-topic", env="SERVER_KAFKA_CONNECTIVITY_TOPIC",
                     default=config["kafka_topics"].get("connectivity", "vehicle-connectivity-v1"))
    add_env_argument(parser, "--kafka-errors-topic", env="SERVER_KAFKA_ERRORS_TOPIC",
                     default=config["kafka_topics"].get("errors", "vehicle-errors-v1"))
    add_env_argument(parser, "--kafka-message-timeout-ms", aliases=("--message-timeout-ms",),
                     dest="kafka_message_timeout_ms", env="SERVER_KAFKA_MESSAGE_TIMEOUT_MS",
                     default=config["kafka"]["message.timeout.ms"], type=int)
    tls = config.get("tls", {})
    add_env_argument(parser, "--server-tls-cert", aliases=("--certfile",), dest="certfile",
                     env="SERVER_TLS_CERT_FILE", default=tls.get("server_cert"), type=project_path)
    add_env_argument(parser, "--server-tls-key", aliases=("--keyfile",), dest="keyfile",
                     env="SERVER_TLS_KEY_FILE", default=tls.get("server_key"), type=project_path)
    add_env_argument(parser, "--server-tls-client-ca", aliases=("--client-ca",), dest="client_ca",
                     env="SERVER_TLS_CLIENT_CA_FILE", default=tls.get("ca_file"), type=project_path)
    add_redis_arguments(parser, config)
    args = parser.parse_args(argv)
    if not 1 <= args.server_port <= 65535 or args.kafka_message_timeout_ms < 1:
        parser.error("Require valid server port and positive Kafka message timeout")
    args.kafka_topics = {
        "V": args.kafka_vehicle_topic, "connectivity": args.kafka_connectivity_topic,
        "errors": args.kafka_errors_topic,
    }
    if (any(not name for name in args.kafka_topics.values())
            or len(set(args.kafka_topics.values())) != len(args.kafka_topics)):
        parser.error("Kafka record types require distinct nonempty topics")
    args.dispatch_records = config["records"]
    args.reliable_ack_sources = config["reliable_ack_sources"]
    args.dispatch_logger_verbose = config["logger"].get("verbose", False)
    return finish_redis_arguments(parser, args, config)


def main():
    args = parse_args()
    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        print("[수신 서버 종료]")


if __name__ == "__main__":
    main()

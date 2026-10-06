"""서버 전용 Kafka Producer. 차량 SQLite에는 접근하지 않습니다."""
import asyncio
import json
from telemetry.record import Record
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from server_dispatcher import DEFAULT_KAFKA_TOPICS, DeliveryError


class Producer:
    def __init__(
        self, bootstrap_servers="localhost:29092,localhost:39092,localhost:49092",
        topic="vehicle-telemetry-v1", message_timeout_ms=30_000,
        max_in_flight=1_000, producer_factory=Producer, topics=None,
    ):
        if message_timeout_ms < 1 or max_in_flight < 1:
            raise ValueError("Require positive timeout and max_in_flight")
        self.topic = topic
        self.topics = dict(DEFAULT_KAFKA_TOPICS if topics is None else topics)
        self.topics["V"] = topic
        if (not self.topics or any(not isinstance(value, str) or not value
                                   for value in self.topics.values())
                or len(set(self.topics.values())) != len(self.topics)):
            raise ValueError("Kafka record types require distinct nonempty topics")
        self.max_in_flight = max_in_flight
        self.pending = {}
        self.producer = producer_factory({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "server-fleet-telemetry",
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "message.timeout.ms": message_timeout_ms,
        })

    @staticmethod
    def _consume_exception(future):
        if not future.cancelled():
            future.exception()

    async def publish(self, event: dict) -> None:
        await self.dispatch("V", event)

    async def dispatch(self, record_type, record):
        """이전 Python 호출과의 호환. 새 코드는 produce(Record)를 사용합니다."""
        await self.produce(Record.from_event(record_type, record))

    async def produce(self, entry: Record) -> None:
        record_type, event = entry.tx_type, entry.data
        if record_type not in self.topics:
            raise DeliveryError(f"No Kafka topic for record type: {record_type}")
        event_id = event["event_id"]
        pending_key = (record_type, event_id)
        # 같은 실행에서 ACK 대기 중인 재접수는 하나의 Kafka 전송에 연결합니다.
        identity = dict(event)
        identity.pop("received_at", None)
        canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, allow_nan=False)
        existing = self.pending.get(pending_key)
        if existing is not None:
            if canonical != existing[0]:
                raise DeliveryError("Conflicting payload for the same event_id")
            await asyncio.shield(existing[1])
            return
        if len(self.pending) >= self.max_in_flight:
            raise DeliveryError("Server Kafka queue is full")

        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(self._consume_exception)
        self.pending[pending_key] = (canonical, future)
        record = dict(identity)
        if record_type != "V":
            record["record_type"] = record_type
        record["received_at"] = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

        def delivered(error, message):
            self.pending.pop(pending_key, None)
            if future.done():
                return
            if error is not None:
                future.set_exception(DeliveryError(str(error)))
            else:
                future.set_result(None)

        try:
            self.producer.produce(
                topic=self.topics[record_type], key=entry.vin.encode("utf-8"),
                value=json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8"),
                on_delivery=delivered,
            )
        except (BufferError, KafkaException) as error:
            self.pending.pop(pending_key, None)
            future.set_exception(DeliveryError(str(error)))
        # 소켓 연결이 끊겨도 이미 접수한 Kafka 전송 자체는 취소하지 않습니다.
        await asyncio.shield(future)

    def poll(self):
        self.producer.poll(0)

    def close(self):
        remaining = self.producer.flush(timeout=10)
        for _, future in self.pending.values():
            future.cancel()
        self.pending.clear()
        print(f"[서버 Kafka 종료] 미완료={remaining}")


if __name__ == "__main__":
    raise SystemExit("수신 서버를 실행하세요: python src/server_fleet_telemetry.py")

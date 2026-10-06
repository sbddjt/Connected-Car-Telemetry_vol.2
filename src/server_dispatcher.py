"""Tesla records/reliable_ack_sources 형태의 비동기 목적지 분배."""
import asyncio
from copy import deepcopy
from telemetry.producer import Producer, Dispatcher
from telemetry.record import Record

RECORD_TYPES = {"V", "connectivity", "errors"}
SUPPORTED_DISPATCHERS = {str(dispatcher) for dispatcher in Dispatcher}
DEFAULT_KAFKA_TOPICS = {
    "V": "vehicle-telemetry-v1",
    "connectivity": "vehicle-connectivity-v1",
    "errors": "vehicle-errors-v1",
}


class DeliveryError(Exception):
    """ACK 기준 목적지에 기록을 확실히 저장하지 못했습니다."""


RecordDispatcher = Producer  # 이전 타입 이름과의 호환


async def produce_record(producer, tx_type, record):
    if hasattr(producer, "produce"):
        await producer.produce(Record.from_event(tx_type, record))
    else:
        # 외부/테스트의 이전 dispatcher 인터페이스도 받을 수 있습니다.
        await producer.dispatch(tx_type, record)


def validate_dispatch_rules(records, reliable_ack_sources, dispatcher_names):
    if not isinstance(records, dict) or not records or set(records) - RECORD_TYPES:
        raise ValueError("records supports V, connectivity and errors")
    for record_type, targets in records.items():
        if (not isinstance(targets, list) or not targets
                or any(not isinstance(name, str) for name in targets)
                or len(set(targets)) != len(targets)
                or set(targets) - set(dispatcher_names)):
            raise ValueError(f"Invalid dispatchers for records.{record_type}")
    # 차량 V는 Kafka 성공 후 ACK 정책을 유지합니다. logger는 ACK 기준이 될 수 없습니다.
    if reliable_ack_sources != {"V": "kafka"} or "kafka" not in records.get("V", []):
        raise ValueError('V requires kafka and reliable_ack_sources={"V": "kafka"}')


class DispatchRouter:
    def __init__(self, dispatchers: dict[str, RecordDispatcher], records: dict,
                 reliable_ack_sources: dict, max_background_tasks=100):
        validate_dispatch_rules(records, reliable_ack_sources, dispatchers)
        if max_background_tasks < 1:
            raise ValueError("max_background_tasks must be positive")
        self.dispatchers = dict(dispatchers)
        self.records = deepcopy(records)
        self.reliable_ack_sources = dict(reliable_ack_sources)
        self.max_background_tasks = max_background_tasks
        self.background_tasks = set()
        self.optional_failures = {}
        self.optional_dropped = {}
        self.closed = False

    def _count(self, counter, record_type, target):
        key = (record_type, target)
        counter[key] = counter.get(key, 0) + 1

    async def _optional_dispatch(self, target, record_type, record):
        try:
            await produce_record(self.dispatchers[target], record_type, record)
        except Exception:
            # 선택 목적지 실패로 차량 ACK를 실패시키거나 무한 재귀 오류 이벤트를 만들지 않습니다.
            self._count(self.optional_failures, record_type, target)

    def _enqueue_optional(self, target, record_type, record):
        if self.closed or len(self.background_tasks) >= self.max_background_tasks:
            self._count(self.optional_dropped, record_type, target)
            return
        task = asyncio.create_task(self._optional_dispatch(target, record_type, deepcopy(record)))
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    def notify(self, record_type: str, record: dict):
        """서버가 생성한 연결/오류 관측 기록: 메모리에 접수하고 바로 반환합니다."""
        if record_type in self.reliable_ack_sources:
            raise ValueError("Reliable records must use dispatch(), not notify()")
        for target in self.records.get(record_type, []):
            self._enqueue_optional(target, record_type, record)

    async def dispatch(self, record_type: str, record: dict) -> None:
        if self.closed:
            raise DeliveryError("Dispatcher router is closed")
        if record_type not in self.records:
            raise DeliveryError(f"No route for record type: {record_type}")
        ack_source = self.reliable_ack_sources.get(record_type)
        for target in self.records[record_type]:
            if target != ack_source:
                self._enqueue_optional(target, record_type, record)
        if ack_source is not None:
            try:
                await produce_record(self.dispatchers[ack_source], record_type, deepcopy(record))
            except DeliveryError:
                raise
            except Exception as error:
                raise DeliveryError(f"{ack_source} dispatch failed: {error}") from error

    async def publish(self, event: dict) -> None:
        await self.dispatch("V", event)

    def poll(self):
        for dispatcher in self.dispatchers.values():
            poll = getattr(dispatcher, "poll", None)
            if poll is not None:
                poll()

    async def close(self, timeout=5.0):
        self.closed = True
        tasks = set(self.background_tasks)
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

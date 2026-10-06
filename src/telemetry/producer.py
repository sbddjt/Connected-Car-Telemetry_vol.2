"""Tesla telemetry/producer.go의 Dispatcher·Producer·BuildTopicName 이름을 따릅니다."""
from enum import StrEnum
from typing import Protocol
from telemetry.record import Record


class Dispatcher(StrEnum):
    Kafka = "kafka"
    Redis = "redis"
    Logger = "logger"


def build_topic_name(namespace, record_name):
    return f"{namespace}_{record_name}"


class Producer(Protocol):
    async def produce(self, entry: Record) -> None:
        """목적지의 성공 조건을 만족할 때 반환합니다. 차량 ACK 기준은 Router가 관리합니다."""

"""Tesla 공개 Redis dispatcher처럼 차량별 Pub/Sub 채널로 배포합니다."""
import asyncio
import json
from telemetry.record import Record
from telemetry.producer import build_topic_name
import time

from redis.asyncio import Redis
from server_dispatcher import DeliveryError, RECORD_TYPES


class Producer:
    def __init__(self, url="redis://127.0.0.1:6380/0", namespace="telemetry_v2",
                 publish_vin_topics=True, subscriber_set_prefix="", timeout_seconds=2.0,
                 client=None, client_options=None, clock=time.time):
        if not namespace or (not publish_vin_topics and not subscriber_set_prefix):
            raise ValueError("Redis requires namespace and publish_vin_topics or subscriber_set_prefix")
        if not 0 < timeout_seconds < float("inf"):
            raise ValueError("Require a positive finite Redis publish timeout")
        self.client = client if client is not None else Redis.from_url(
            url, **(client_options or {"decode_responses": True, "protocol": 2,
                                      "socket_connect_timeout": timeout_seconds,
                                      "socket_timeout": timeout_seconds}),
        )
        self.namespace = namespace
        self.publish_vin_topics = publish_vin_topics
        self.subscriber_set_prefix = subscriber_set_prefix
        self.timeout_seconds = timeout_seconds
        self.clock = clock
        self.published_count = 0

    def vin_channel(self, tx_type, vin):
        if tx_type not in RECORD_TYPES:
            raise ValueError("Unsupported Redis record type")
        return f"{build_topic_name(self.namespace, tx_type)}_{{{vin}}}"

    async def channels_for_record(self, record_type, record):
        channel = self.vin_channel(record_type, record["vehicle_id"])
        channels = []
        if self.subscriber_set_prefix:
            key = f"{self.subscriber_set_prefix}_{channel}"
            # 공개 코드와 같이 현재 초보다 작은 만료 lease만 제거합니다.
            await self.client.zremrangebyscore(key, "-inf", f"({int(self.clock())}")
            channels.extend(await self.client.zrange(key, 0, -1))
        if self.publish_vin_topics:
            channels.append(channel)
        return channels

    async def dispatch(self, record_type, record):
        """이전 Python 호출과의 호환. 새 코드는 produce(Record)를 사용합니다."""
        await self.produce(Record.from_event(record_type, record))

    async def produce(self, entry: Record):
        record_type, record = entry.tx_type, entry.data
        payload = json.dumps(record, ensure_ascii=False, allow_nan=False)
        try:
            async with asyncio.timeout(self.timeout_seconds):
                for channel in await self.channels_for_record(record_type, record):
                    # 0 subscribers도 Redis의 정상 PUBLISH 결과입니다. 소비/저장을 보장하지 않습니다.
                    await self.client.publish(channel, payload)
                    self.published_count += 1
        except Exception as error:
            raise DeliveryError(f"Redis publish failed: {type(error).__name__}") from error

    async def close(self):
        await self.client.aclose()

    vehicle_channel = vin_channel  # 이전 Python 호출과의 호환

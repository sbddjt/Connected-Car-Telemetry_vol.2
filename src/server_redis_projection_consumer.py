"""우리 조회 기능: 독립 Kafka 그룹에서 Redis 최신 상태를 만들고 재시도합니다."""
import argparse
import json
import time
from collections import deque

from confluent_kafka import Consumer, KafkaException
from redis.exceptions import RedisError

from server_redis_latest_store import RedisLatestVehicleStore
from shared_fleet_telemetry_policy import Backoff
from shared_redis_config import add_redis_arguments, finish_redis_arguments
from shared_runtime_config import add_env_argument, server_config_argument


def project_and_commit(message, store, consumer):
    event = json.loads(message.value().decode("utf-8"))
    store.save(event)
    consumer.commit(message=message, asynchronous=False)


class RedisProjectionWorker:
    def __init__(self, consumer, store, retry_seconds=1.0, retry_max_seconds=30.0):
        self.consumer = consumer
        self.store = store
        self.pending = deque()
        self.retry = Backoff(retry_seconds, retry_max_seconds)
        self.paused = False

    def on_revoke(self, consumer, partitions):
        revoked = {(partition.topic, partition.partition) for partition in partitions}
        # 소유권을 잃은 기록은 새 소유자가 미커밋 오프셋부터 처리합니다.
        self.pending = deque(
            message for message in self.pending
            if (message.topic(), message.partition()) not in revoked
        )

    def process_once(self):
        if self.pending and time.monotonic() >= self.retry.after:
            message = self.pending[0]
            try:
                project_and_commit(message, self.store, self.consumer)
            except (RedisError, KafkaException) as error:
                delay = self.retry.fail(time.monotonic())
                partitions = self.consumer.assignment()
                if partitions:
                    self.consumer.pause(partitions)
                self.paused = True
                print(f"[Redis 조회 상태 재시도] {type(error).__name__} | {delay:.1f}초 후")
            else:
                self.pending.popleft()
                self.retry.reset()
                if self.paused:
                    partitions = self.consumer.assignment()
                    if partitions:
                        self.consumer.resume(partitions)
                    self.paused = False
        if self.paused and not self.pending:
            partitions = self.consumer.assignment()
            if partitions:
                self.consumer.resume(partitions)
            self.paused = False
            self.retry.reset()
        # Redis 장애 중에도 poll로 그룹 이벤트/재할당을 처리합니다.
        # pause 이전에 이미 나온 메시지는 버리지 않고 순서대로 보관합니다.
        message = self.consumer.poll(0.2)
        if message is not None and not message.error():
            self.pending.append(message)
        elif message is not None:
            print(f"[Redis Consumer Kafka 재연결 대기] {message.error()}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Kafka → Redis 신호별 최신 상태 (별도 소비 그룹)")
    config = server_config_argument(parser, argv)
    add_env_argument(parser, "--kafka-bootstrap-servers", env="SERVER_KAFKA_BOOTSTRAP_SERVERS",
                     default=config["kafka"]["bootstrap.servers"])
    add_env_argument(parser, "--kafka-vehicle-topic", env="SERVER_KAFKA_VEHICLE_TOPIC",
                     default=config["kafka_topics"]["V"])
    add_env_argument(parser, "--kafka-redis-consumer-group", env="SERVER_KAFKA_REDIS_CONSUMER_GROUP_ID",
                     default="telemetry-redis-latest-v1")
    add_redis_arguments(parser, config)
    args = parser.parse_args(argv)
    return finish_redis_arguments(parser, args, config)


def main():
    args = parse_args()
    store = RedisLatestVehicleStore(
        args.server_redis_url, args.redis_cache_prefix, client_options=args.redis_client_options,
    )
    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "group.id": args.kafka_redis_consumer_group,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    worker = RedisProjectionWorker(consumer, store)
    try:
        consumer.subscribe([args.kafka_vehicle_topic], on_revoke=worker.on_revoke, on_lost=worker.on_revoke)
        print(f"[Redis 최신 상태 갱신] topic={args.kafka_vehicle_topic} | group={args.kafka_redis_consumer_group}")
        while True:
            worker.process_once()
    except KeyboardInterrupt:
        print("[Redis Consumer 종료] 미완료 오프셋은 다음 실행에서 처리합니다.")
    finally:
        consumer.close()
        store.close()


if __name__ == "__main__":
    main()

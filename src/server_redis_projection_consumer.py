"""우리 조회 기능: 독립 Kafka 그룹에서 Redis 최신 상태를 만들고 재시도합니다."""
import argparse
import json

from confluent_kafka import Consumer, KafkaException
from redis.exceptions import RedisError

from server_redis_latest_store import RedisLatestVehicleStore
from shared_redis_config import add_redis_arguments, finish_redis_arguments
from shared_runtime_config import add_env_argument, server_config_argument


def project_and_commit(message, store, consumer):
    event = json.loads(message.value().decode("utf-8"))
    store.save(event)
    consumer.commit(message=message, asynchronous=False)


from shared_kafka_storage_worker import KafkaStorageWorker


class RedisProjectionWorker(KafkaStorageWorker):
    def __init__(self, consumer, store, retry_seconds=1, retry_max_seconds=30, batch_size=1):
        super().__init__(consumer, store, "redis-consumer", (RedisError,), batch_size=batch_size,
                         retry_seconds=retry_seconds, retry_max_seconds=retry_max_seconds)


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
    worker = RedisProjectionWorker(consumer, store, batch_size=500)
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

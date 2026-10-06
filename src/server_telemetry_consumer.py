"""Kafka 기록을 이력·최신 상태에 저장한 뒤 소비 오프셋을 커밋합니다."""
import argparse
import json
from pathlib import Path

from confluent_kafka import Consumer

from server_telemetry_store import ServerTelemetryStore
from shared_runtime_config import add_env_argument, project_path, server_config_argument

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def persist_and_commit(message, store, consumer):
    event = json.loads(message.value().decode("utf-8"))
    store.save(event)
    consumer.commit(message=message, asynchronous=False)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Kafka → 서버 SQLite 이력 및 최신 상태 저장")
    config = server_config_argument(parser, argv)
    add_env_argument(parser, "--server-storage-db", aliases=("--db-path",), dest="server_storage_db",
                     env="SERVER_SQLITE_STORAGE_DB_PATH",
                     default=PROJECT_ROOT / "data" / "server_telemetry.db", type=project_path)
    add_env_argument(parser, "--kafka-bootstrap-servers", aliases=("--bootstrap-servers",),
                     dest="kafka_bootstrap_servers", env="SERVER_KAFKA_BOOTSTRAP_SERVERS",
                     default=config["kafka"]["bootstrap.servers"])
    add_env_argument(parser, "--kafka-vehicle-topic", aliases=("--topic",), dest="kafka_vehicle_topic",
                     env="SERVER_KAFKA_VEHICLE_TOPIC", default=config["kafka_topics"]["V"])
    add_env_argument(parser, "--kafka-storage-consumer-group", aliases=("--group-id",), dest="kafka_storage_consumer_group",
                     env="SERVER_KAFKA_STORAGE_CONSUMER_GROUP_ID", default="telemetry-storage-v1")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    store = ServerTelemetryStore(args.server_storage_db)
    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "group.id": args.kafka_storage_consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    try:
        consumer.subscribe([args.kafka_vehicle_topic])
        print(f"[서버 저장 시작] db={args.server_storage_db} | topic={args.kafka_vehicle_topic}")
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"[Kafka 소비 재연결 대기] {message.error()}")
                continue
            # 데이터 검증·DB 저장·커밋 실패 시 중단합니다. 해당 오프셋은 넘기지 않습니다.
            persist_and_commit(message, store, consumer)
    except KeyboardInterrupt:
        print("[서버 저장 종료]")
    finally:
        consumer.close()
        store.close()


if __name__ == "__main__":
    main()

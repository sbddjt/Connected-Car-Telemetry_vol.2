"""Kafka 기록을 MongoDB 이력에 저장한 뒤 소비 오프셋을 커밋합니다."""
import argparse
import json
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
from pymongo.errors import ConnectionFailure, ExecutionTimeout, WriteConcernError
from server_mongodb_history_store import MongoDBHistoryStore

from shared_kafka_storage_worker import KafkaStorageWorker
from server_telemetry_store import ServerTelemetryStore
from shared_runtime_config import add_env_argument, project_path, server_config_argument

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def persist_and_commit(message, store, consumer):
    event = json.loads(message.value().decode("utf-8"))
    store.save(event)
    consumer.commit(message=message, asynchronous=False)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Kafka → MongoDB 이력 저장 (SQLite는 이전 검증 재현용)")
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
                     env="SERVER_KAFKA_STORAGE_CONSUMER_GROUP_ID", default="telemetry-history-mongodb-v1")
    add_env_argument(parser, "--server-history-backend", env="SERVER_HISTORY_STORAGE_BACKEND", default="mongodb")
    add_env_argument(parser, "--mongodb-uri", env="SERVER_MONGODB_URI", default="mongodb://127.0.0.1:27018")
    add_env_argument(parser, "--mongodb-database", env="SERVER_MONGODB_DATABASE", default="vehicle_telemetry")
    add_env_argument(parser, "--mongodb-history-collection", env="SERVER_MONGODB_HISTORY_COLLECTION", default="telemetry_history")
    args = parser.parse_args(argv)
    if args.server_history_backend not in {"mongodb", "sqlite"}:
        parser.error("server-history-backend supports mongodb or legacy sqlite")
    if not args.mongodb_uri.startswith(("mongodb://", "mongodb+srv://")):
        parser.error("Require mongodb:// or mongodb+srv:// URI")
    return args


class HistoryConsumerWorker(KafkaStorageWorker):
    def __init__(self, consumer, store, batch_size=500):
        super().__init__(consumer, store, "history-consumer",
                         (ConnectionFailure, ExecutionTimeout, WriteConcernError), batch_size=batch_size)


def main():
    args = parse_args()
    store = (MongoDBHistoryStore(args.mongodb_uri, args.mongodb_database, args.mongodb_history_collection)
             if args.server_history_backend == "mongodb" else ServerTelemetryStore(args.server_storage_db))
    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "group.id": args.kafka_storage_consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    worker = HistoryConsumerWorker(consumer, store, batch_size=500 if args.server_history_backend == "mongodb" else 1)
    try:
        consumer.subscribe([args.kafka_vehicle_topic], on_revoke=worker.on_revoke, on_lost=worker.on_revoke)
        print(f"[서버 이력 저장 시작] backend={args.server_history_backend} | topic={args.kafka_vehicle_topic} | group={args.kafka_storage_consumer_group}")
        while True:
            worker.process_once()
    except KeyboardInterrupt:
        print("[서버 저장 종료]")
    finally:
        consumer.close()
        store.close()


if __name__ == "__main__":
    main()

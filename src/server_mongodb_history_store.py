"""서버 이력 역할: 재전송에 안전하게 차량 이벤트를 MongoDB에 저장합니다."""
import json
from datetime import datetime, timezone
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, WriteConcernError, ConnectionFailure
from pymongo.write_concern import WriteConcern
from shared_fleet_telemetry_policy import utc_timestamp, validate_event


class MongoDBHistoryStore:
    def __init__(self, uri="mongodb://127.0.0.1:27018", database="vehicle_telemetry",
                 collection="telemetry_history", client=None):
        self.client = client if client is not None else MongoClient(
            uri, serverSelectionTimeoutMS=1500, connectTimeoutMS=1500,
            socketTimeoutMS=15000, tz_aware=True, retryWrites=False)
        # 로컬 단일 노드의 디스크 저널 확인까지 기다립니다. 다중 노드에서는 majority에도 적용됩니다.
        self.collection = self.client[database].get_collection(
            collection, write_concern=WriteConcern(w="majority", j=True, wtimeout=2000))
        self.indexes_ready = False

    def save(self, event):
        self.save_many([event])

    def save_many(self, events):
        events = list(events)
        if not events:
            return
        for event in events:
            validate_event(event)
        if not self.indexes_ready:
            self.collection.create_index([("vehicle_id", ASCENDING), ("event_time", ASCENDING)],
                                         name="vehicle_observation_time")
            self.indexes_ready = True
        operations = []
        for event in events:
            identity = dict(event)
            identity.pop("received_at", None)
            canonical = json.dumps(identity, sort_keys=True, ensure_ascii=False, allow_nan=False)
            document = {
                "_id": event["event_id"], "event_id": event["event_id"],
                "vehicle_id": event["vehicle_id"], "run_id": event["run_id"],
                "sequence_no": event["sequence_no"],
                "event_time": datetime.fromisoformat(utc_timestamp(event["event_time"]).replace("Z", "+00:00")),
                "stored_at": datetime.now(timezone.utc), "signals": event["signals"],
                "event": event, "payload_identity": canonical,
            }
            operations.append(UpdateOne({"_id":event["event_id"], "payload_identity":canonical},
                                        {"$setOnInsert":document}, upsert=True))
        # A rebalance can briefly overlap writes by the old and new partition owner.
        # A duplicate-key upsert race is retryable only when the stored payload matches.
        for attempt in range(3):
            try:
                self.collection.bulk_write(operations, ordered=True)
                return
            except BulkWriteError as error:
                write_errors = error.details.get("writeErrors", [])
                if not write_errors:
                    raise WriteConcernError("MongoDB journal acknowledgement failed") from error
                for failure in write_errors:
                    event = events[failure["index"]]
                    if failure.get("code") != 11000:
                        raise ValueError("Invalid history record: " + event["event_id"]) from error
                    stored = self.collection.find_one({"_id": event["event_id"]})
                    identity = dict(event)
                    identity.pop("received_at", None)
                    canonical = json.dumps(identity, sort_keys=True, ensure_ascii=False, allow_nan=False)
                    if stored and stored.get("payload_identity") != canonical:
                        raise ValueError("Conflicting history record: " + event["event_id"]) from error
        raise ConnectionFailure("Concurrent history upsert did not settle; retry batch")

    def close(self):
        self.client.close()

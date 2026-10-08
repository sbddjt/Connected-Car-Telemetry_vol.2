"""Run in query-api via kubectl exec -i ... -- python - ; stores two marked test events."""
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from uuid import uuid4

sys.path.insert(0, "/app/src")
from pymongo import MongoClient
from websockets.asyncio.client import connect
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from server_redis_latest_store import RedisLatestVehicleStore

async def main():
    run_id = "k8s-verification-" + str(uuid4())
    vehicle_id = run_id
    events = [{"event_id": f"{run_id}:{number}", "vehicle_id": vehicle_id,
               "run_id": run_id, "sequence_no": number,
               "event_time": datetime.now(timezone.utc).isoformat(),
               "signals": {"speed_mps": float(number),
                           "location": {"latitude": 37.498 + number / 10000, "longitude": 127.027}}}
              for number in (1, 2)]
    expected = {event["event_id"] for event in events}
    with tempfile.TemporaryDirectory(prefix="k8s-delivery-") as directory:
        buffer = VehicleSQLiteBuffer(Path(directory) / "buffer.db", overflow_policy="block")
        try:
            buffer.save_many(events)
            async with connect("ws://receiver:8765") as socket:
                await socket.send(json.dumps({"type": "hello", "vehicle_id": vehicle_id}))
                assert json.loads(await asyncio.wait_for(socket.recv(), 30))["type"] == "ready"
                for event in events + [events[0]]:
                    await socket.send(json.dumps({"type": "telemetry", "event": event}))
                    reply = json.loads(await asyncio.wait_for(socket.recv(), 45))
                    assert reply.get("type") == "ack" and reply.get("event_id") == event["event_id"], reply
                    buffer.acknowledge(reply["event_id"])
            assert buffer.get_pending() == [], "Unacknowledged records remain"
        finally:
            buffer.close()

    client = MongoClient(os.environ["SERVER_MONGODB_URI"], serverSelectionTimeoutMS=3000)
    redis_store = RedisLatestVehicleStore(os.environ["SERVER_REDIS_URL"])
    try:
        history = client.vehicle_telemetry.telemetry_history
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            actual = {document["_id"] for document in history.find({"run_id": run_id}, {"_id": 1})}
            latest = redis_store.get_vehicle(vehicle_id)
            if actual == expected and latest and latest["signals"].get("speed_mps") == 2.0:
                print(json.dumps({"run_id": run_id, "expected_events": 2, "mongo_unique_events": len(actual),
                    "websocket_acks": 3, "sqlite_pending": 0, "redis_latest_speed_mps": 2.0,
                    "missing_event_ids": sorted(expected - actual), "result": "passed"}))
                return
            await asyncio.sleep(.5)
        raise RuntimeError("Expected event IDs or Redis latest state were not observed")
    finally:
        redis_store.close()
        client.close()

if __name__ == "__main__":
    asyncio.run(main())

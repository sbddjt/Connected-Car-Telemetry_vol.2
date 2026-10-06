"""한 PC 실험의 차량 버퍼 쓰기 잠금을 여러 SQLite 파일로 분산합니다."""
import hashlib
from pathlib import Path
from vehicle_sqlite_buffer import VehicleSQLiteBuffer


def vehicle_buffer_shard(vehicle_id, shards):
    if shards < 1:
        raise ValueError("Require positive shard count")
    return int.from_bytes(hashlib.sha256(vehicle_id.encode("utf-8")).digest()[:8],"big") % shards


def shard_path(directory, index, shards):
    if not 0 <= index < shards:
        raise ValueError("Invalid vehicle buffer shard index")
    return Path(directory) / f"shard-{index:02d}.db"


class ShardedVehicleSQLiteBuffer:
    def __init__(self, directory, shards=8, max_events=5000):
        self.shards = shards
        self.buffers = [VehicleSQLiteBuffer(shard_path(directory,i,shards),max_events=max_events)
                        for i in range(shards)]

    def save_many(self, events):
        buckets = [[] for _ in self.buffers]
        for event in events:
            buckets[vehicle_buffer_shard(event["vehicle_id"],self.shards)].append(event)
        # 원자성은 각 파일의 묶음에 적용됩니다. 서로 다른 파일 사이의 전역 트랜잭션은 아닙니다.
        for buffer, bucket in zip(self.buffers,buckets):
            buffer.save_many(bucket)

    def close(self):
        for buffer in self.buffers:
            buffer.close()

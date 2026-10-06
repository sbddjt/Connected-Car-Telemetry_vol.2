import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from vehicle_sharded_buffer import ShardedVehicleSQLiteBuffer,vehicle_buffer_shard,shard_path
from vehicle_sqlite_buffer import VehicleSQLiteBuffer


class ShardedBufferTests(unittest.TestCase):
    def test_stable_vehicle_assignment_and_ack_only_in_its_shard(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            buffer=ShardedVehicleSQLiteBuffer(root,shards=8)
            records=[{"event_id":f"id-{i}","vehicle_id":f"car-{i}"} for i in range(80)]
            buffer.save_many(records)
            self.assertEqual(sum(len(store.get_pending()) for store in buffer.buffers),80)
            for record in records:
                index=vehicle_buffer_shard(record["vehicle_id"],8)
                self.assertIn(record["event_id"],{row["event_id"] for row in buffer.buffers[index].get_pending()})
            target=records[0];index=vehicle_buffer_shard(target["vehicle_id"],8)
            buffer.buffers[index].acknowledge(target["event_id"])
            self.assertEqual(sum(len(store.get_pending()) for store in buffer.buffers),79)
            buffer.close()
            reopened=VehicleSQLiteBuffer(shard_path(root,index,8))
            self.assertNotIn(target["event_id"],{record["event_id"] for record in reopened.get_pending()})
            reopened.close()

    def test_capacity_is_per_vehicle_inside_a_shared_shard(self):
        with tempfile.TemporaryDirectory() as directory:
            buffer=ShardedVehicleSQLiteBuffer(Path(directory),shards=1,max_events=2)
            buffer.save_many([{"event_id":f"{car}-{i}","vehicle_id":car} for car in ("a","b") for i in range(3)])
            self.assertEqual(len(buffer.buffers[0].get_pending()),4)
            buffer.close()


if __name__=="__main__":unittest.main()

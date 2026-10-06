import json
import sys
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import Mock, patch
from confluent_kafka import KafkaException, TopicPartition
from pymongo.errors import ConnectionFailure
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from shared_kafka_storage_worker import KafkaStorageWorker
from test_server_kafka_producer import event


class Message:
    def __init__(self, offset, partition=0): self.number,self.part=offset,partition
    def value(self): return json.dumps(event(self.number+1)).encode()
    def topic(self): return "vehicles"
    def partition(self): return self.part
    def offset(self): return self.number
    def error(self): return None


class BatchStorageTests(unittest.TestCase):
    def setUp(self):
        patcher=patch("shared_kafka_storage_worker.PipelineMetrics")
        patcher.start();self.addCleanup(patcher.stop)
        self.consumer=Mock()
        self.consumer.assignment.return_value=[TopicPartition("vehicles",0),TopicPartition("vehicles",1)]
        self.consumer.consume.return_value=[]
        self.store=Mock()
        self.worker=KafkaStorageWorker(self.consumer,self.store,"history-consumer",(ConnectionFailure,),batch_size=3)
        self.messages=[Message(4),Message(5),Message(9,1)]
        self.worker.pending.extend(self.messages)

    def test_all_records_stored_before_partition_offsets_commit(self):
        self.worker.process_once()
        self.store.save_many.assert_called_once()
        offsets=self.consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual({(p.partition,p.offset) for p in offsets},{(0,6),(1,10)})
        self.assertEqual(len(self.worker.pending),0)
        self.assertEqual(self.worker.total,3)

    def test_partial_storage_failure_keeps_entire_batch_without_commit(self):
        self.store.save_many.side_effect=ConnectionFailure("offline")
        self.worker.process_once()
        self.consumer.commit.assert_not_called()
        self.assertEqual(list(self.worker.pending),self.messages)
        self.assertTrue(self.worker.paused)
        self.store.save_many.side_effect=None;self.worker.retry.after=0
        self.worker.process_once()
        self.assertEqual(self.worker.total,3)
        self.assertFalse(self.worker.paused)

    def test_commit_failure_replays_saved_batch(self):
        self.consumer.commit.side_effect=KafkaException()
        self.worker.process_once()
        self.assertEqual(list(self.worker.pending),self.messages)
        self.consumer.commit.side_effect=None;self.worker.retry.after=0
        self.worker.process_once()
        self.assertEqual(self.store.save_many.call_count,2)
        self.assertEqual(len(self.worker.pending),0)

    def test_revoke_excludes_records_that_belong_to_new_owner(self):
        self.worker.on_revoke(self.consumer,[TopicPartition("vehicles",0)])
        self.worker.process_once()
        offsets=self.consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual([(p.partition,p.offset) for p in offsets],[(1,10)])


if __name__=="__main__": unittest.main()

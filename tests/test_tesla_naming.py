import contextlib
import io
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from telemetry.record import Record
from telemetry.producer import Dispatcher, build_topic_name
from datastore.kafka.kafka import Producer as KafkaProducer
from datastore.redis.redis import Producer as RedisProducer
from datastore.simple.logger import Producer as LoggerProducer
from server_kafka_dispatcher import KafkaDispatcher
from server_redis_dispatcher import RedisDispatcher
from server_logger_dispatcher import LoggerDispatcher
from server_fleet_telemetry import parse_args
from test_server_kafka_dispatcher import event, FakeKafka


class TeslaNamingTests(unittest.IsolatedAsyncioTestCase):
    def test_record_and_dispatcher_names_map_without_rewriting_json(self):
        data=event(); entry=Record.from_event("V",data)
        self.assertEqual((entry.vin,entry.tx_type,entry.txid),(data["vehicle_id"],"V",data["event_id"]))
        self.assertIs(entry.data,data)
        self.assertEqual({str(name) for name in Dispatcher},{"kafka","redis","logger"})
        self.assertEqual(build_topic_name("telemetry_v2","V"),"telemetry_v2_V")
        self.assertIs(KafkaDispatcher,KafkaProducer)
        self.assertIs(RedisDispatcher,RedisProducer)
        self.assertIs(LoggerDispatcher,LoggerProducer)

    async def test_produce_record_uses_vin_as_kafka_key(self):
        native=FakeKafka(); producer=KafkaProducer(producer_factory=native.factory)
        import asyncio
        task=asyncio.create_task(producer.produce(Record.from_event("V",event())))
        await asyncio.sleep(0)
        self.assertEqual(native.queue[0]["key"],b"car-1")
        producer.poll(); await task
        self.assertEqual(len(native.delivered),1)
        producer.close()

    def test_tesla_password_precedence_and_config_flag_alias(self):
        with patch.dict(os.environ,{"REDIS_PASSWORD":"canonical","SERVER_REDIS_PASSWORD":"legacy"}):
            args=parse_args(["--config","config/server_fleet_telemetry_config.json"])
            self.assertEqual(args.redis_client_options["password"],"canonical")
            self.assertEqual(args.redis_publish_timeout,5_000_000_000)
            legacy=parse_args(["--server-telemetry-config","config/server_fleet_telemetry_config.json"])
            self.assertEqual(args.server_telemetry_config,legacy.server_telemetry_config)
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):
            parse_args(["--redis-publish-timeout","0"])

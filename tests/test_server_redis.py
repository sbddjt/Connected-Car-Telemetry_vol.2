import asyncio
import contextlib
import http.client
import io
import json
import sys
import threading
import unittest
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import fakeredis
from fakeredis.aioredis import FakeRedis as AsyncFakeRedis
from confluent_kafka import TopicPartition, KafkaException
from redis.exceptions import ConnectionError, ResponseError
from server_dispatcher import DispatchRouter, DeliveryError
from server_redis_dispatcher import RedisDispatcher
from server_redis_latest_store import RedisLatestVehicleStore
from server_redis_projection_consumer import RedisProjectionWorker, project_and_commit, parse_args as projection_args
from server_vehicle_query_api import make_query_handler, parse_args as query_args
from server_fleet_telemetry import parse_args as receiver_args
from test_server_kafka_dispatcher import event, until


class RedisDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_server = fakeredis.FakeServer()
        self.client = AsyncFakeRedis(server=self.fake_server, decode_responses=True)
        self.dispatcher = RedisDispatcher(client=self.client)
        self.addAsyncCleanup(self.dispatcher.close)

    async def test_vehicle_channel_carries_raw_record(self):
        async with self.client.pubsub() as subscriber:
            await subscriber.subscribe("telemetry_v2_V_{car-1}")
            await subscriber.get_message(timeout=1)
            original = event()
            await self.dispatcher.dispatch("V", original)
            message = await subscriber.get_message(ignore_subscribe_messages=True, timeout=1)
            self.assertEqual(json.loads(message["data"]), original)
            self.assertEqual(message["channel"], "telemetry_v2_V_{car-1}")

    async def test_zero_subscribers_is_success(self):
        await self.dispatcher.dispatch("V", event())
        self.assertEqual(self.dispatcher.published_count, 1)
        self.assertEqual(self.dispatcher.vehicle_channel("errors", "car-1"), "telemetry_v2_errors_{car-1}")

    async def test_subscriber_leases_expire_strictly_before_current_second(self):
        dispatcher = RedisDispatcher(client=self.client, subscriber_set_prefix="leases", clock=lambda:100)
        key = "leases_telemetry_v2_V_{car-1}"
        await self.client.zadd(key, {"expired":99, "at-boundary":100, "future":101})
        self.assertEqual(await dispatcher.channels_for_record("V", event()),
                         ["at-boundary", "future", "telemetry_v2_V_{car-1}"])
        self.assertEqual(await self.client.zrange(key, 0, -1), ["at-boundary", "future"])

    async def test_leased_channels_can_replace_vehicle_channel(self):
        dispatcher = RedisDispatcher(client=self.client, publish_vin_topics=False,
                                     subscriber_set_prefix="leases", clock=lambda:100)
        await self.client.zadd("leases_telemetry_v2_V_{car-1}", {"dashboard-channel":200})
        self.assertEqual(await dispatcher.channels_for_record("V", event()), ["dashboard-channel"])
        with self.assertRaises(ValueError):
            RedisDispatcher(publish_vin_topics=False)

    async def test_redis_down_does_not_block_kafka_vehicle_ack(self):
        self.fake_server.connected = False
        class Kafka:
            async def dispatch(self, kind, record):
                pass
        router = DispatchRouter({"kafka":Kafka(), "redis":self.dispatcher},
                                {"V":["kafka","redis"]}, {"V":"kafka"})
        try:
            await asyncio.wait_for(router.publish(event()), .5)
            await until(lambda:router.optional_failures.get(("V","redis")) == 1)
        finally:
            await router.close()

    async def test_publish_timeout_is_bounded(self):
        class SlowRedis:
            async def publish(self, *args):
                await asyncio.Event().wait()
        dispatcher = RedisDispatcher(client=SlowRedis(), timeout_seconds=.02)
        with self.assertRaises(DeliveryError):
            await asyncio.wait_for(dispatcher.dispatch("V", event()), .5)


class RedisStoreTests(unittest.TestCase):
    def setUp(self):
        self.client = fakeredis.FakeRedis(decode_responses=True)
        self.store = RedisLatestVehicleStore(client=self.client)
        self.addCleanup(self.store.close)

    def test_late_history_and_duplicate_do_not_rewind_latest(self):
        self.assertEqual(self.store.save(event(10)), 2)
        self.assertEqual(self.store.save(event(1)), 0)
        self.assertEqual(self.store.save(event(10)), 0)
        self.assertEqual(self.store.get_vehicle("car-1")["signals"]["speed_mps"], 10)
        self.assertEqual(self.client.zcard(self.store.index_key), 1)

    def test_partial_signal_updates_preserve_independent_location_time(self):
        self.store.save(event(10, signals={"location":{"latitude":37,"longitude":127}}))
        self.store.save(event(5, signals={"speed_mps":5}))
        result=self.store.get_vehicle("car-1")
        self.assertEqual(result["signals"], {"location":{"latitude":37,"longitude":127},"speed_mps":5})
        self.assertNotEqual(result["observations"]["location"]["event_time"], result["observations"]["speed_mps"]["event_time"])

    def test_utc_normalization_and_large_sequence_tie_break(self):
        first=event(); first.update(event_time="2026-10-06T09:00:01.000001+09:00", sequence_no=2**60)
        next_record=event(2); next_record.update(event_time="2026-10-06T00:00:01.000001Z", sequence_no=2**60+1)
        self.store.save(first); self.store.save(next_record)
        latest=self.store.get_vehicle("car-1")["observations"]["speed_mps"]
        self.assertEqual(latest["sequence_no"], str(2**60+1))
        different_run=event(3); different_run.update(event_time=next_record["event_time"], run_id="other", sequence_no=2**61)
        self.assertEqual(self.store.save(different_run), 0)

    def test_wrong_index_type_has_no_partial_vehicle_write(self):
        self.client.set(self.store.index_key, "wrong-type")
        with self.assertRaises(ResponseError):
            self.store.save(event())
        self.assertFalse(self.client.exists(self.store.vehicle_key("car-1")))

    def test_corrupt_existing_signal_has_no_partial_updates(self):
        self.store.save(event())
        self.client.hset(self.store.vehicle_key("car-1"), "speed_mps", "invalid-json")
        before=self.client.hgetall(self.store.vehicle_key("car-1"))
        with self.assertRaises(ResponseError):
            self.store.save(event(2))
        self.assertEqual(self.client.hgetall(self.store.vehicle_key("car-1")), before)

    def test_atomic_concurrent_writers_end_with_newest_record(self):
        with ThreadPoolExecutor(max_workers=6) as executor:
            list(executor.map(self.store.save, [event(i) for i in range(20,0,-1)]))
        self.assertEqual(self.store.get_vehicle("car-1")["signals"]["speed_mps"], 20)

    def test_list_pagination_and_encoded_vehicle_keys(self):
        self.store.save(event(vehicle="car/a% b")); self.store.save(event(vehicle="z"))
        self.assertTrue(self.store.get_vehicle("car/a% b"))
        self.assertIn("car%2Fa%25%20b", self.store.vehicle_key("car/a% b"))
        result=self.store.list_vehicles(limit=1,offset=1)
        self.assertEqual(result["total"],2)
        self.assertEqual([v["vehicle_id"] for v in result["vehicles"]], ["z"])
        with self.assertRaises(ValueError): self.store.list_vehicles(201)


class Message:
    def __init__(self, record=None, partition=0):
        self.record=record or event()
        self.part=partition
    def value(self): return json.dumps(self.record).encode()
    def topic(self): return "vehicles"
    def partition(self): return self.part
    def error(self): return None


class Consumer:
    def __init__(self, messages=()):
        self.messages=deque(messages); self.committed=[]; self.polls=0
        self.pauses=0; self.resumes=0; self.commit_error=None
    def poll(self, timeout):
        self.polls+=1
        return self.messages.popleft() if self.messages else None
    def commit(self, message, asynchronous):
        if self.commit_error: raise self.commit_error
        assert asynchronous is False
        self.committed.append(message)
    def assignment(self): return [TopicPartition("vehicles",0),TopicPartition("vehicles",1)]
    def pause(self, partitions): self.pauses+=1
    def resume(self, partitions): self.resumes+=1


class RedisProjectionTests(unittest.TestCase):
    def setUp(self):
        self.fake_server=fakeredis.FakeServer()
        self.client=fakeredis.FakeRedis(server=self.fake_server,decode_responses=True)
        self.store=RedisLatestVehicleStore(client=self.client)
        self.addCleanup(self.store.close)

    def test_offset_only_commits_after_redis_write(self):
        consumer=Consumer(); message=Message()
        self.fake_server.connected=False
        with self.assertRaises(ConnectionError): project_and_commit(message,self.store,consumer)
        self.assertEqual(consumer.committed,[])
        self.fake_server.connected=True
        project_and_commit(message,self.store,consumer)
        self.assertEqual(consumer.committed,[message])
        self.assertIsNotNone(self.store.get_vehicle("car-1"))

    def test_commit_failure_replay_is_idempotent(self):
        consumer=Consumer(); message=Message()
        consumer.commit_error=KafkaException()
        with self.assertRaises(KafkaException): project_and_commit(message,self.store,consumer)
        consumer.commit_error=None
        project_and_commit(message,self.store,consumer)
        self.assertEqual(self.client.zcard(self.store.index_key),1)
        self.assertEqual(consumer.committed,[message])

    def test_outage_pauses_but_polls_and_recovery_keeps_buffered_records(self):
        first,second=Message(event()),Message(event(2))
        consumer=Consumer([first,second]); worker=RedisProjectionWorker(consumer,self.store)
        worker.process_once()
        self.fake_server.connected=False
        with contextlib.redirect_stdout(io.StringIO()): worker.process_once()
        self.assertTrue(worker.paused); self.assertEqual(consumer.polls,2)
        self.assertEqual(list(worker.pending),[first,second]); self.assertEqual(consumer.committed,[])
        self.fake_server.connected=True; worker.retry.after=0
        worker.process_once(); worker.process_once()
        self.assertEqual(consumer.committed,[first,second]); self.assertFalse(worker.paused)

    def test_revoked_pending_records_not_committed_and_empty_queue_resumes(self):
        consumer=Consumer(); worker=RedisProjectionWorker(consumer,self.store)
        worker.pending.append(Message()); worker.paused=True; worker.retry.after=float("inf")
        worker.on_revoke(consumer,[TopicPartition("vehicles",0)])
        worker.process_once()
        self.assertEqual(consumer.committed,[]); self.assertFalse(worker.paused)
        self.assertEqual(consumer.resumes,1)

    def test_partial_revocation_keeps_owned_records(self):
        consumer=Consumer(); worker=RedisProjectionWorker(consumer,self.store)
        owned=Message(event(2),1); worker.pending.extend([Message(),owned])
        worker.on_revoke(consumer,[TopicPartition("vehicles",0)])
        worker.process_once()
        self.assertEqual(consumer.committed,[owned])


class RedisQueryAPITests(unittest.TestCase):
    def setUp(self):
        self.fake_server=fakeredis.FakeServer()
        self.client=fakeredis.FakeRedis(server=self.fake_server,decode_responses=True)
        self.store=RedisLatestVehicleStore(client=self.client)
        self.server=ThreadingHTTPServer(("127.0.0.1",0),make_query_handler(self.store))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.addCleanup(self.stop)
    def stop(self):
        self.server.query_stopping=True; self.server.shutdown(); self.server.server_close(); self.thread.join(); self.store.close()
    def request(self,path):
        connection=http.client.HTTPConnection("127.0.0.1",self.server.server_port,timeout=3)
        try:
            connection.request("GET",path); response=connection.getresponse()
            return response.status,response.read(),dict(response.getheaders())
        finally: connection.close()

    def test_static_screen_and_vehicle_field_selection(self):
        self.store.save(event())
        code,body,headers=self.request("/")
        self.assertEqual(code,200); self.assertIn("차량 최신 정보",body.decode()); self.assertIn("Content-Security-Policy",headers)
        code,body,_=self.request("/api/vehicles?signals=speed_mps")
        self.assertEqual(code,200)
        result=json.loads(body); self.assertEqual(set(result["vehicles"][0]["signals"]),{"speed_mps"})
        self.assertEqual(self.request("/vehicle_state.js")[0],200)

    def test_lookup_encoded_id_and_validation_errors(self):
        self.store.save(event(vehicle="a/b% c"))
        self.assertEqual(self.request("/api/vehicles/a%2Fb%25%20c")[0],200)
        self.assertEqual(self.request("/api/vehicles/missing")[0],404)
        for path in ("/api/vehicles?limit=201","/api/vehicles?offset=-1","/api/vehicles?signals=secret"):
            self.assertEqual(self.request(path)[0],400)

    def test_redis_down_returns_503_for_cache_health_and_stream(self):
        self.fake_server.connected=False
        for path in ("/api/vehicles","/api/health","/api/stream"):
            code,body,_=self.request(path)
            self.assertEqual(code,503,path); self.assertIn("temporarily unavailable",body.decode())

    def test_pubsub_is_streamed_as_sse_with_safe_sequence(self):
        connection=http.client.HTTPConnection("127.0.0.1",self.server.server_port,timeout=3)
        try:
            connection.request("GET","/api/stream?vehicle_id=car-1"); response=connection.getresponse()
            self.assertEqual(response.status,200)
            self.assertEqual(response.readline(),b": connected\n"); response.readline()
            record=event(); record["sequence_no"]=2**60+1
            record["event_time"]="2026-10-06T09:00:01.000001+09:00"
            self.client.publish("telemetry_v2_V_{car-1}",json.dumps(record))
            data=response.readline().decode()
            self.assertTrue(data.startswith("data: "))
            self.assertEqual(json.loads(data[6:])["sequence_no"],str(2**60+1))
            self.assertEqual(json.loads(data[6:])["event_time"],"2026-10-06T00:00:01.000001Z")
        finally: connection.close()

    def test_api_registers_lease_and_receives_only_its_destination_channel(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.server=ThreadingHTTPServer(("127.0.0.1",0), make_query_handler(
            self.store, publish_vin_topics=False, subscriber_set_prefix="leases"))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.assertEqual(self.request("/api/stream")[0],400)
        connection=http.client.HTTPConnection("127.0.0.1",self.server.server_port,timeout=3)
        try:
            connection.request("GET","/api/stream?vehicle_id=car-1"); response=connection.getresponse()
            self.assertEqual(response.status,200); response.readline(); response.readline()
            channels=self.client.zrange("leases_telemetry_v2_V_{car-1}",0,-1)
            self.assertEqual(len(channels),1)
            self.client.publish(channels[0],json.dumps(event()))
            self.assertEqual(json.loads(response.readline().decode()[6:])["vehicle_id"],"car-1")
        finally: connection.close()


class RedisConfigTests(unittest.TestCase):
    def test_shared_connection_env_and_separate_kafka_group(self):
        with patch.dict("os.environ",{"SERVER_REDIS_URL":"redis://localhost:6388/2","SERVER_REDIS_NAMESPACE":"test","SERVER_REDIS_PASSWORD":"password"}):
            for parse in (receiver_args,projection_args,query_args):
                args=parse([])
                self.assertEqual(args.server_redis_url,"redis://localhost:6388/2")
                self.assertEqual(args.redis_client_options["password"],"password")
            self.assertEqual(projection_args([]).kafka_redis_consumer_group,"telemetry-redis-latest-v1")
    def test_invalid_url_and_timeout_rejected(self):
        for args in (["--redis-url","https://localhost"],["--redis-socket-timeout-seconds","nan"]):
            with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit): query_args(args)


if __name__=="__main__": unittest.main()

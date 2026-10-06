"""실제 SUMO·SQLite·WebSocket·Kafka·Redis 검증. 장애 주입은 명시적 옵션입니다."""
import argparse
import asyncio
import contextlib
import http.client
import json
import sys
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.backoff import NoBackoff
from redis.retry import Retry
from server_fleet_telemetry import FleetTelemetryServer, build_dispatch_router, parse_args
from datastore.simple.logger import Producer as LoggerProducer
from server_redis_latest_store import RedisLatestVehicleStore
from server_redis_projection_consumer import RedisProjectionWorker
from server_telemetry_consumer import persist_and_commit
from server_telemetry_store import ServerTelemetryStore
from server_vehicle_query_api import make_query_handler
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from vehicle_fleet_telemetry_client import SimulatedFleetTelemetryClients
from websockets.asyncio.server import serve
from websockets.asyncio.client import connect

BOOTSTRAP = "localhost:29092,localhost:39092,localhost:49092"
SERVICES = ("redis-query", "kafka-1", "kafka-2", "kafka-3")


async def docker(*args):
    process = await asyncio.create_subprocess_exec("docker", *args, cwd=ROOT,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    output, _ = await process.communicate()
    if process.returncode: raise RuntimeError(output.decode(errors="replace"))
    return output.decode()


async def wait_until(condition, label, seconds=60):
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline: raise AssertionError("Timeout: " + label)
        await asyncio.sleep(.05)


def request(server, path):
    connection=http.client.HTTPConnection("127.0.0.1",server.server_port,timeout=10)
    try:
        connection.request("GET",path); response=connection.getresponse()
        return response.status, response.read()
    finally: connection.close()


def synthetic(run_id, number, seconds=0):
    return {"run_id":run_id,"event_id":f"{run_id}:verification-car:{number}",
        "vehicle_id":"verification-car","sequence_no":number,
        "event_time":(datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat(),
        "signals":{"location":{"latitude":37.5+number/10000,"longitude":127.0},"speed_mps":float(number)}}


async def main(fault_injection, report_path):
    token=uuid4().hex[:12]; namespace="verification_"+token
    topics={kind:f"{namespace}_{kind}" for kind in ("V","connectivity","errors")}
    admin=AdminClient({"bootstrap.servers":BOOTSTRAP,"socket.timeout.ms":5000})
    metadata=await asyncio.to_thread(admin.list_topics,timeout=15)
    assert len(metadata.brokers)==3, "Require the three-broker test stack"
    for future in admin.create_topics([NewTopic(topic,3,3,config={"min.insync.replicas":"2"}) for topic in topics.values()]).values():
        await asyncio.to_thread(future.result,20)
    # Never stop an unrelated Compose stack during fault injection.
    if fault_injection:
        ids=(await docker("compose","ps","-q",*SERVICES)).split()
        assert len(ids)==4, "Require all four vol.2 containers"
        containers=json.loads(await docker("inspect",*ids))
        assert all(item["Config"]["Labels"].get("com.docker.compose.project")=="telemetry-v2" and
            Path(item["Config"]["Labels"]["com.docker.compose.project.working_dir"]).resolve()==ROOT.resolve()
            for item in containers), "Refuse to stop another project's containers"
    report={"time_kst":datetime.now(timezone(timedelta(hours=9))).isoformat(),
        "brokers":len(metadata.brokers),"namespace":namespace,"checks":{}}
    tasks=[]; consumers=[]; stopped=set(); stop=asyncio.Event(); logger_lines=[]; published=[]
    options={"decode_responses":True,"protocol":2,"socket_timeout":1,"socket_connect_timeout":1,
             "retry":Retry(NoBackoff(),0)}
    cache=RedisLatestVehicleStore(prefix=f"{namespace}:{{query}}",client_options=options)
    subscriber=AsyncRedis.from_url("redis://127.0.0.1:6380/0",decode_responses=True,protocol=2)
    pubsub=subscriber.pubsub(ignore_subscribe_messages=True)
    router=None; server=None; http_server=None
    with tempfile.TemporaryDirectory(prefix="telemetry-v2-verification-") as temporary:
        directory=Path(temporary)
        buffer=VehicleSQLiteBuffer(directory/"vehicle.db")
        history=ServerTelemetryStore(directory/"history.db")
        config=json.loads((ROOT/"config/server_fleet_telemetry_config.json").read_text(encoding="utf-8"))
        config["kafka_topics"]=topics; config["namespace"]=namespace
        config_path=directory/"config.json"; config_path.write_text(json.dumps(config),encoding="utf-8")
        args=parse_args(["--config",str(config_path),"--kafka-message-timeout-ms","1500"])
        router=build_dispatch_router(args,logger_dispatcher=LoggerProducer(writer=logger_lines.append))
        def consumer(suffix,topic):
            client=Consumer({"bootstrap.servers":BOOTSTRAP,"group.id":namespace+suffix,
                "auto.offset.reset":"earliest","enable.auto.commit":False,"enable.auto.offset.store":False,
                "socket.timeout.ms":3000})
            client.subscribe([topic]); consumers.append(client); return client
        history_consumer=consumer("_history",topics["V"])
        query_consumer=consumer("_query",topics["V"])
        operations=[consumer("_"+kind,topics[kind]) for kind in ("connectivity","errors")]
        worker=RedisProjectionWorker(query_consumer,cache,retry_seconds=.25,retry_max_seconds=2)
        query_consumer.subscribe([topics["V"]],on_revoke=worker.on_revoke,on_lost=worker.on_revoke)
        seen=set(); operational=[]; keys=[]
        async def poll():
            while not stop.is_set():
                router.poll()
                message=history_consumer.poll(0)
                if message is not None and not message.error():
                    persist_and_commit(message,history,history_consumer)
                    seen.add(json.loads(message.value())["event_id"])
                    keys.append(message.key().decode())
                for client in operations:
                    message=client.poll(0)
                    if message is not None and not message.error():
                        operational.append(json.loads(message.value()))
                        client.commit(message=message,asynchronous=False)
                await asyncio.sleep(.01)
        async def project():
            while not stop.is_set():
                await asyncio.to_thread(worker.process_once)
        async def watch():
            while not stop.is_set():
                message=await pubsub.get_message(timeout=.2)
                if message and message["type"]=="pmessage": published.append(json.loads(message["data"]))
                await asyncio.sleep(.01)
        async def collect():
            sumo=ET.Element("configuration"); inputs=ET.SubElement(sumo,"input")
            for name,file in (("net-file","osm.net.xml.gz"),("route-files","osm.passenger.trips.xml")):
                ET.SubElement(inputs,name,value=str(ROOT/"scenario/gangnam"/file))
            processing=ET.SubElement(sumo,"processing"); ET.SubElement(processing,"ignore-route-errors",value="true")
            config_file=directory/"sumo.sumocfg"; ET.ElementTree(sumo).write(config_file,encoding="utf-8")
            process=await asyncio.create_subprocess_exec(sys.executable,str(ROOT/"src/vehicle_sumo_collector.py"),
                "--sumo-config",str(config_file),"--sumo-binary","sumo","--sumo-end-seconds","4",
                "--vehicle-buffer-db",str(directory/"vehicle.db"),
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT,cwd=ROOT)
            output,_=await process.communicate()
            if process.returncode: raise RuntimeError(output.decode(errors="replace"))
            return output.decode(errors="replace")
        try:
            server=await serve(FleetTelemetryServer(router).handle,"127.0.0.1",0)
            fleet=SimulatedFleetTelemetryClients(buffer,
                fleet_telemetry_server_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                ack_timeout=4,retry_seconds=.25,retry_max_seconds=2)
            print("[live] SUMO roundtrip",flush=True)
            await pubsub.psubscribe(namespace+"_V_*"); await pubsub.get_message(timeout=1)
            tasks=[asyncio.create_task(poll()),asyncio.create_task(project())]
            await collect()
            initial=buffer.get_pending(limit=10000); assert initial, "SUMO produced no selected records"
            expected={record["event_id"] for record in initial}
            watcher=asyncio.create_task(watch()); tasks.append(watcher)
            tasks.append(asyncio.create_task(fleet.run(stop)))
            await wait_until(lambda:not buffer.get_pending() and expected<=seen,"SUMO to Kafka/history")
            first=initial[0]
            await wait_until(lambda:cache.get_vehicle(first["vehicle_id"]) is not None,"Redis latest state")
            await wait_until(lambda:expected<={r["event_id"] for r in published},"VIN Pub/Sub delivery")
            assert set(keys)>={record["vehicle_id"] for record in initial}
            assert logger_lines
            report["checks"]["sumo_roundtrip"]={"messages":len(expected),"vehicles":len({r["vehicle_id"] for r in initial}),
                "buffer_remaining":len(buffer.get_pending()),"pubsub_messages":len(published),"logger_lines":len(logger_lines)}
            http_server=ThreadingHTTPServer(("127.0.0.1",0),make_query_handler(cache,namespace))
            thread=threading.Thread(target=http_server.serve_forever,daemon=True); thread.start()
            status,body=await asyncio.to_thread(request,http_server,"/api/vehicles?signals=location")
            assert status==200 and json.loads(body)["vehicles"]
            report["checks"]["redis_query_api"]={"status":status}
            watcher.cancel(); await asyncio.gather(watcher,return_exceptions=True)
            if fault_injection:
                print("[live] Redis outage/recovery",flush=True)
                await docker("compose","stop","redis-query"); stopped.add("redis-query")
                record=synthetic(token,20); buffer.save(record); expected.add(record["event_id"])
                await wait_until(lambda:record["event_id"] in seen and not buffer.get_pending(),"Kafka ACK with Redis down")
                await wait_until(lambda:worker.paused,"query projection paused")
                status,_=await asyncio.to_thread(request,http_server,"/api/vehicles")
                assert status==503
                report["checks"]["redis_outage"]={"kafka_ack":True,"projection_paused":worker.paused,"query_status":status}
                await docker("compose","start","redis-query"); stopped.discard("redis-query")
                await asyncio.sleep(2)
                await wait_until(lambda:cache.get_vehicle("verification-car") is not None,"Redis projection recovery")
                report["checks"]["redis_recovery"]={"projected":True}
                older=synthetic(token,1,seconds=-3600); buffer.save(older); expected.add(older["event_id"])
                await wait_until(lambda:older["event_id"] in seen and not buffer.get_pending(),"late record history")
                await wait_until(lambda:not worker.pending,"late record projection")
                assert cache.get_vehicle("verification-car")["signals"]["speed_mps"]==20
                report["checks"]["late_history"]={"history_saved":True,"latest_speed_mps":20}
                print("[live] Kafka outage/recovery",flush=True)
                await docker("compose","stop","kafka-1","kafka-2","kafka-3")
                stopped.update(("kafka-1","kafka-2","kafka-3"))
                offline=synthetic(token,30); buffer.save(offline); expected.add(offline["event_id"])
                await collect()
                retained=buffer.get_pending(limit=10000)
                assert any(r["event_id"]==offline["event_id"] for r in retained)
                assert any(r["vehicle_id"]!="verification-car" for r in retained)
                expected.update(r["event_id"] for r in retained)
                report["checks"]["kafka_outage"]={"buffered_messages":len(retained),"sumo_continued":True}
                await docker("compose","start","kafka-1","kafka-2","kafka-3")
                stopped.difference_update(("kafka-1","kafka-2","kafka-3"))
                await wait_until(lambda:not buffer.get_pending() and expected<=seen,"all buffered events after Kafka recovery",120)
                await wait_until(lambda:cache.get_vehicle("verification-car")["signals"]["speed_mps"]==30,"query catchup",60)
                # 선택 오류 기록은 Kafka 장애 중 유실 가능하므로 정상 상태에서 검증 오류를 생성합니다.
                print("[live] operational routing after recovery",flush=True)
                async with connect(f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",proxy=None) as socket:
                    await socket.send(json.dumps({"type":"hello","vehicle_id":"verification-validation"}))
                    await socket.recv()
                    invalid=synthetic(token,40); invalid["vehicle_id"]="verification-validation"
                    invalid["signals"]={"speed_mps":"invalid"}
                    await socket.send(json.dumps({"type":"telemetry","event":invalid}))
                    reply=json.loads(await socket.recv()); assert reply["type"]=="error"
                await wait_until(lambda:{r["record_type"] for r in operational}>={"connectivity","errors"},"operational topic routing")
                report["checks"]["kafka_recovery"]={"all_expected_history":expected<=seen,"buffer_remaining":len(buffer.get_pending()),
                    "unique_history_messages":len(seen),"latest_speed_mps":30,"operational_topics":["connectivity","errors"]}
            report["passed"]=True
        finally:
            if stopped: await docker("compose","start",*sorted(stopped))
            stop.set()
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            if server: server.close(); await server.wait_closed()
            if router:
                await router.close(); await router.dispatchers["redis"].close(); router.dispatchers["kafka"].close()
            for client in consumers: await asyncio.to_thread(client.close)
            if http_server:
                http_server.query_stopping=True; await asyncio.to_thread(http_server.shutdown); http_server.server_close()
            await pubsub.aclose(); await subscriber.aclose()
            scratch_keys=list(cache.client.scan_iter(namespace+":*"))
            if scratch_keys: cache.client.delete(*scratch_keys)
            cache.close(); history.close(); buffer.close()
            for future in admin.delete_topics(list(topics.values())).values():
                await asyncio.to_thread(future.result,20)
    report_path.parent.mkdir(parents=True,exist_ok=True)
    report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fault-injection",action="store_true",help="vol.2 Redis와 Kafka를 실제 중단·재시작")
    parser.add_argument("--report",type=Path,default=ROOT/"data/live-verification.json")
    arguments=parser.parse_args()
    asyncio.run(main(arguments.fault_injection,arguments.report))

"""별도 Kafka 토픽·MongoDB 컬렉션으로 저장/중복/DB 단절 복구를 검증합니다."""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from server_mongodb_history_store import MongoDBHistoryStore
from server_telemetry_consumer import HistoryConsumerWorker


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fault-injection",action="store_true")
    parser.add_argument("--report",type=Path,default=ROOT/"data/runtime/mongodb-verification.json")
    args=parser.parse_args()
    suffix=uuid4().hex
    topic="verify-mongodb-"+suffix
    collection="verify_history_"+suffix
    group="verify-mongodb-"+suffix
    brokers="localhost:29092,localhost:39092,localhost:49092"
    admin=AdminClient({"bootstrap.servers":brokers})
    admin.create_topics([NewTopic(topic,2,3)])[topic].result(30)
    store=MongoDBHistoryStore(collection=collection)
    consumer=Consumer({"bootstrap.servers":brokers,"group.id":group,"auto.offset.reset":"earliest",
                       "enable.auto.commit":False,"enable.auto.offset.store":False})
    producer=Producer({"bootstrap.servers":brokers,"acks":"all","enable.idempotence":True})
    worker=HistoryConsumerWorker(consumer,store,batch_size=500)
    events=[]
    for i in range(100):
        events.append({"event_id":f"{suffix}:{i}","vehicle_id":"verify-car","run_id":suffix,
                       "sequence_no":i+1,"event_time":datetime.now(timezone.utc).isoformat(),
                       "simulation_time":i,"signals":{"location":{"latitude":37.5,"longitude":127.04},"speed_mps":10}})
    failures=[]
    def send(records):
        for event in records:
            producer.produce(topic,key=event["vehicle_id"],value=json.dumps(event),
                             on_delivery=lambda error,msg:failures.append(str(error)) if error else None)
        if producer.flush(20) or failures:
            raise AssertionError("Kafka delivery failed")
    def until(predicate,timeout=40):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            worker.process_once()
            if predicate():return
        raise AssertionError("History recovery timed out")
    def committed():
        return sum(max(0,p.offset) for p in consumer.committed(consumer.assignment(),timeout=5))
    stopped=False
    try:
        consumer.subscribe([topic],on_revoke=worker.on_revoke,on_lost=worker.on_revoke)
        send(events[:60])
        until(lambda:store.collection.count_documents({})==60)
        first_offsets=committed()
        if args.fault_injection:
            subprocess.run(["docker","compose","stop","-t","3","mongodb-history"],cwd=ROOT,check=True,capture_output=True)
            stopped=True
            send(events[60:])
            deadline=time.monotonic()+15
            while not worker.paused and time.monotonic()<deadline:worker.process_once()
            assert worker.paused,"DB failure should pause consumption"
            assert committed()==first_offsets,"Unstored records must not commit offsets"
            subprocess.run(["docker","compose","start","mongodb-history"],cwd=ROOT,check=True,capture_output=True)
            stopped=False
            deadline=time.monotonic()+30
            while True:
                try:store.client.admin.command("ping");break
                except Exception:
                    if time.monotonic()>deadline:raise
                    time.sleep(.5)
            worker.retry.after=0
        else:
            send(events[60:])
        until(lambda:store.collection.count_documents({})==100)
        send(events[:3]);until(lambda:worker.total>=103)
        assert store.collection.count_documents({})==100,"Replay should deduplicate"
        saved=store.collection.find_one({"_id":events[0]["event_id"]})
        assert isinstance(saved["event_time"],datetime)
        assert saved["signals"]["speed_mps"]==10
        assert store.collection.write_concern.document=={"w":"majority","j":True,"wtimeout":2000}
        conflict=dict(events[0]);conflict["signals"]={"speed_mps":99}
        try:store.save(conflict)
        except ValueError:pass
        else:raise AssertionError("Conflicting payload must be rejected")
        report={"passed":True,"kafka_records_processed":worker.total,"unique_history_documents":100,
                "mongodb_outage_tested":args.fault_injection,"offsets_before_outage":first_offsets,
                "offsets_after_recovery":committed(),"write_concern":store.collection.write_concern.document}
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
        print(json.dumps(report))
    finally:
        if stopped:subprocess.run(["docker","compose","start","mongodb-history"],cwd=ROOT,check=True,capture_output=True)
        consumer.close()
        # Names generated above belong exclusively to this verification run.
        store.client["vehicle_telemetry"].drop_collection(collection)
        store.close()
        admin.delete_topics([topic])[topic].result(20)


if __name__=="__main__":main()

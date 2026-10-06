"""실행 중인 확장 시나리오의 운행 대수·처리량·지연을 일정 구간 측정합니다."""
import argparse
import json
import time
import urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds",type=int,default=60)
    parser.add_argument("--url",default="http://127.0.0.1:8092")
    parser.add_argument("--report",type=Path,default=ROOT/"data/runtime/expanded-load-report.json")
    args=parser.parse_args()
    if args.seconds<5:parser.error("Measure at least 5 seconds")
    def read():
        with urllib.request.urlopen(args.url+"/api/pipeline",timeout=15) as response:
            return json.load(response)["components"]
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        try:
            components=read()
        except (OSError,TimeoutError):
            time.sleep(2)
            continue
        collector=components.get("vehicle-collector") or {}
        if not collector.get("stale") and collector.get("active_vehicles",0)>=300:break
        time.sleep(3)
    else:raise AssertionError("300 active vehicles were not observed within warmup")
    print("300 active vehicles observed; sampling load window",flush=True)
    samples=[];deadline=time.monotonic()+args.seconds
    while time.monotonic()<deadline:
        try:
            samples.append(read())
        except (OSError,TimeoutError):
            pass
        time.sleep(5)
    if not samples:raise AssertionError("No monitoring samples were received")
    def values(component,key):
        return [sample[component][key] for sample in samples
                if sample.get(component) and not sample[component].get("stale")
                and isinstance(sample[component].get(key),(int,float))]
    report={"measurement_seconds":args.seconds,"samples":len(samples),"components":{}}
    for component in ["vehicle-collector","vehicle-sender","history-consumer","redis-consumer"]:
        rates=values(component,"events_per_second")
        delays=values(component,"processing_delay_seconds")
        report["components"][component]={"average_events_per_second":round(sum(rates)/len(rates),1) if rates else None,
                    "max_processing_delay_seconds":round(max(delays),1) if delays else None,
                    "reporting_workers":(samples[-1].get(component) or {}).get("reporting_workers")}
    active=values("vehicle-collector","active_vehicles")
    pending=values("vehicle-sender","pending_events")
    report.update(min_active_vehicles=min(active) if active else None,max_active_vehicles=max(active) if active else None,
                  pending_events_start=pending[0] if pending else None,pending_events_end=pending[-1] if pending else None,
                  capacity_dropped=(samples[-1].get("vehicle-sender") or {}).get("capacity_dropped"),
                  collection_step_p95_seconds=(samples[-1].get("vehicle-collector") or {}).get("collection_step_p95_seconds"))
    report["raw_samples"]=samples
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({key:value for key,value in report.items() if key!="raw_samples"},ensure_ascii=False))


if __name__=="__main__":main()

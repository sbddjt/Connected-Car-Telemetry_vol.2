"""로컬 모니터링 스냅샷. 기록·ACK·오프셋 처리의 성공 조건에는 포함하지 않습니다."""
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
METRICS_NAMES = ("vehicle-collector", "vehicle-sender", "history-consumer", "redis-consumer")


def metrics_directory():
    directory = Path(os.environ.get("SHARED_PIPELINE_METRICS_DIRECTORY", "data/runtime/metrics"))
    return directory if directory.is_absolute() else ROOT / directory


class PipelineMetrics:
    def __init__(self, name, directory=None, interval=5):
        if name not in METRICS_NAMES:
            raise ValueError("Unknown metrics producer")
        instance = os.environ.get("SHARED_PIPELINE_METRICS_INSTANCE", "")
        if instance and not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}",instance):
            raise ValueError("Invalid metrics instance")
        filename = name + ("-"+instance if instance else "") + ".json"
        self.path = (directory if directory is not None else metrics_directory()) / filename
        self.interval = interval
        self.after = 0
        self.started = time.monotonic()
        self.previous_at = self.started
        self.previous_total = 0
        self.failures = 0

    def publish(self, total, force=False, **values):
        now = time.monotonic()
        if not force and now < self.after:
            return
        elapsed = max(now - self.previous_at, .001)
        record = {"updated_at": datetime.now(timezone.utc).isoformat(),
                  "total": total, "events_per_second": round(max(0, total-self.previous_total)/elapsed, 1),
                  "uptime_seconds": round(now-self.started, 1), **values}
        temporary = self.path.with_suffix(".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(json.dumps(record, ensure_ascii=False, allow_nan=False), encoding="utf-8")
            temporary.replace(self.path)
        except (OSError, ValueError):
            self.failures += 1
            if self.failures == 1:
                logging.warning("Monitoring snapshot write failed: %s", self.path.name)
        else:
            self.failures = 0
        self.previous_at, self.previous_total = now, total
        self.after = now+self.interval


def read_pipeline_metrics(directory=None):
    root = directory if directory is not None else metrics_directory()
    result = {}
    now = datetime.now(timezone.utc)
    for name in METRICS_NAMES:
        records = []
        instances = list(root.glob(name+"-*.json"))
        paths = instances if instances else [root / (name+".json")]
        for path in paths:
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                record["stale"] = (now-datetime.fromisoformat(record["updated_at"])).total_seconds() > 15
                records.append(record)
            except (OSError,ValueError,TypeError,KeyError):
                continue
        fresh = [record for record in records if not record["stale"]]
        if not fresh:
            result[name] = {**max(records,key=lambda record:record["updated_at"]),"stale":True} if records else None
            continue
        combined = dict(max(fresh,key=lambda record:record["updated_at"]))
        for field in ("total","events_per_second","pending_events","capacity_dropped","active_workers","in_flight","pending_messages"):
            if any(field in record for record in fresh):
                combined[field] = round(sum(record.get(field,0) for record in fresh),1)
        delays = [record["processing_delay_seconds"] for record in fresh if record.get("processing_delay_seconds") is not None]
        if delays:
            combined["processing_delay_seconds"] = max(delays)
        combined["retrying"] = any(record.get("retrying",False) for record in fresh)
        combined["reporting_workers"] = len(fresh)
        result[name] = combined
    return {"components":result,"sample_interval_seconds":5}

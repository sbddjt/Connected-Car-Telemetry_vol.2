"""Tesla 공개 동작과 프로젝트에서 선택한 값을 한곳에서 관리합니다."""
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone

# Tesla 공개 정책: 500ms 묶음, 차량당 5000개 메시지, 재연결 최대 30초.
BUCKET_SECONDS = 0.5
BUFFER_MESSAGES_PER_VEHICLE = 5_000
RECONNECT_MAX_SECONDS = 30.0

# 프로젝트 선택: 위치·속도의 최소 기록 간격, 전송 비율, 동시에 기다릴 ACK 수.
SIGNAL_INTERVALS = {"location": 1.0, "speed_mps": 1.0}
LIVE_WEIGHT = 3
MAX_IN_FLIGHT_PER_VEHICLE = 20


def signals_of(event: dict) -> dict:
    """이전 버전의 전체 스냅샷도 전송·저장할 수 있도록 해석합니다."""
    if "signals" in event:
        return event["signals"]
    signals = {}
    if "latitude" in event and "longitude" in event:
        signals["location"] = {
            "latitude": event["latitude"], "longitude": event["longitude"],
        }
    if "speed_mps" in event:
        signals["speed_mps"] = event["speed_mps"]
    return signals


def utc_timestamp(value: str) -> str:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("event_time must include a timezone")
    return stamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_event(event) -> dict:
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    for field in ("event_id", "vehicle_id", "run_id", "event_time"):
        if not isinstance(event.get(field), str) or not 1 <= len(event[field]) <= 512:
            raise ValueError(f"Invalid {field}")
    utc_timestamp(event["event_time"])
    if type(event.get("sequence_no")) is not int or event["sequence_no"] < 1:
        raise ValueError("sequence_no must be a positive integer")
    signals = signals_of(event)
    if not isinstance(signals, dict) or not signals:
        raise ValueError("signals must be a nonempty object")
    if set(signals) - set(SIGNAL_INTERVALS):
        raise ValueError("Unknown signal")
    if "location" in signals:
        location = signals["location"]
        if not isinstance(location, dict) or set(location) != {"latitude", "longitude"}:
            raise ValueError("Invalid location")
        for key, bound in (("latitude", 90), ("longitude", 180)):
            number = location[key]
            if type(number) not in (int, float) or not math.isfinite(number) or abs(number) > bound:
                raise ValueError(f"Invalid {key}")
    if "speed_mps" in signals:
        number = signals["speed_mps"]
        if type(number) not in (int, float) or not math.isfinite(number) or number < 0:
            raise ValueError("Invalid speed_mps")
    # 커밋/전송 전 JSON으로 표현할 수 있는지 확인합니다.
    json.dumps(event, allow_nan=False)
    return event


class SignalCollector:
    """마지막으로 기록한 값과 달라지고 최소 간격이 지난 신호만 선택합니다."""
    def __init__(self, intervals: dict[str, float] | None = None):
        self.intervals = dict(SIGNAL_INTERVALS if intervals is None else intervals)
        if not self.intervals or any(
            key not in SIGNAL_INTERVALS or not math.isfinite(value) or value <= 0
            for key, value in self.intervals.items()
        ):
            raise ValueError("Require known signals with positive finite intervals")
        self.emitted: dict[tuple[str, str], tuple[float, object]] = {}

    def collect(self, vehicle_id: str, sample: dict, now: float) -> dict:
        selected = {}
        for signal, interval in self.intervals.items():
            if signal not in sample:
                continue
            key = (vehicle_id, signal)
            previous = self.emitted.get(key)
            if previous is None or (
                now - previous[0] >= interval and sample[signal] != previous[1]
            ):
                selected[signal] = sample[signal]
        return selected

    def committed(self, vehicle_id: str, selected: dict, now: float) -> None:
        # SQLite 커밋 성공 후에만 간격·변경 비교 기준을 진행합니다.
        for signal, value in selected.items():
            self.emitted[(vehicle_id, signal)] = (now, value)


@dataclass
class Backoff:
    initial: float = 1.0
    maximum: float = RECONNECT_MAX_SECONDS
    attempt: int = 0
    delay: float = 0.0
    after: float = 0.0

    def __post_init__(self):
        if not (math.isfinite(self.initial) and math.isfinite(self.maximum)
                and 0 < self.initial <= self.maximum):
            raise ValueError("Require finite 0 < initial <= maximum")

    def fail(self, now: float) -> float:
        self.attempt += 1
        self.delay = self.initial if self.attempt == 1 else min(self.maximum, self.delay * 2)
        self.after = now + self.delay
        return self.delay

    def reset(self) -> None:
        self.attempt = 0
        self.delay = 0.0
        self.after = 0.0

"""Kafka와 독립적으로 수신 기록을 콘솔에 출력하는 선택 목적지."""
import asyncio
import json
from telemetry.record import Record


class Producer:
    def __init__(self, verbose=False, writer=print):
        self.verbose = verbose
        self.writer = writer

    async def dispatch(self, record_type, record):
        """이전 Python 호출과의 호환. 새 코드는 produce(Record)를 사용합니다."""
        await self.produce(Record.from_event(record_type, record))

    async def produce(self, entry: Record) -> None:
        record_type, record = entry.tx_type, entry.data
        if self.verbose:
            output = {"record_type": record_type, "record": record}
        else:
            output = {"record_type": record_type}
            for key in ("event_id", "vehicle_id", "event_time", "status", "stage", "error"):
                if key in record:
                    output[key] = record[key]
            if record_type == "V":
                output["signal_names"] = list(record.get("signals", {}))
        line = json.dumps(output, ensure_ascii=False, allow_nan=False)
        # 콘솔 쓰기로 WebSocket/Kafka 이벤트 루프를 막지 않습니다.
        await asyncio.to_thread(self.writer, line)

"""Tesla Record 용어를 사용하는 서버 envelope; 로컬 JSON/SQLite는 그대로 보존합니다."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    vin: str
    tx_type: str
    txid: str
    data: dict

    @classmethod
    def from_event(cls, tx_type, data):
        return cls(vin=data["vehicle_id"], tx_type=tx_type, txid=data["event_id"], data=data)

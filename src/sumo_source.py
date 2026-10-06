import time
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4
from event_buffer import EventBuffer

import traci

from kafka_producer import TelemetryProducer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUMO_CONFIG = PROJECT_ROOT / "scenario" / "gangnam" / "osm.sumocfg"

STEP_SECONDS = 1.0


def main() -> None:
    run_id = str(uuid4())
    vehicle_sequences: dict[str, int] = {}

    # 1. SQLite 버퍼 연결
    buffer = EventBuffer(
        PROJECT_ROOT / "data" / "event_buffer.db"
    )
    producer = None
    sumo_started = False

    try:
        # 2. Producer가 같은 SQLite 버퍼를 사용하도록 전달
        producer = TelemetryProducer(buffer)

        # 3. 이전 실행에서 남은 미완료 이벤트부터 전송
        producer.drain_pending()

        # 4. 복구가 끝나면 SUMO 실행
        traci.start([
            "sumo-gui",
            "-c",
            str(SUMO_CONFIG),
            "--step-length",
            str(STEP_SECONDS),
        ])
        sumo_started = True

        while traci.simulation.getMinExpectedNumber() > 0:
            step_started_at = time.monotonic()

            # 시뮬레이션 시간을 1초 진행
            traci.simulationStep()

            simulation_time = traci.simulation.getTime()
            vehicle_ids = list(traci.vehicle.getIDList())

            events = []

            for vehicle_id in vehicle_ids:
                x, y = traci.vehicle.getPosition(vehicle_id)
                longitude, latitude = traci.simulation.convertGeo(x, y)
                sequence_no = vehicle_sequences.get(vehicle_id, 0) + 1

                event_time = datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                ).replace("+00:00", "Z")

                events.append({
                    "run_id": run_id,
                    "event_id": f"{run_id}:{vehicle_id}:{sequence_no}",
                    "vehicle_id": vehicle_id,
                    "sequence_no" : sequence_no,
                    "event_time" : event_time,
                    "simulation_time": simulation_time,
                    "speed_mps": traci.vehicle.getSpeed(vehicle_id),
                    "acceleration_mps2": traci.vehicle.getAcceleration(vehicle_id),
                    "latitude": latitude,
                    "longitude": longitude,
                    "angle": traci.vehicle.getAngle(vehicle_id),
                    "road_id": traci.vehicle.getRoadID(vehicle_id),
                    "lane_id": traci.vehicle.getLaneID(vehicle_id),
                })
                buffer.save(events[-1])
                vehicle_sequences[vehicle_id] = sequence_no

            print(
                f"time={simulation_time:>6.1f}s | "
                f"active={len(vehicle_ids):>4} | "
                f"events={len(events):>4}"
            )

            producer.drain_pending()

            # 시뮬레이션 1초와 실제 1초를 맞춤
            elapsed = time.monotonic() - step_started_at
            time.sleep(max(0, STEP_SECONDS - elapsed))

    finally:
        try:
            if producer is not None:
                producer.close()
        finally:
            try:
                if sumo_started:
                    traci.close()
            finally:
                buffer.close()

if __name__ == "__main__":
    main()

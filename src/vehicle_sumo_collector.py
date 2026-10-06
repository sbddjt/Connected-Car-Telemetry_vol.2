"""차량 역할: SUMO 신호를 선택해 차량 SQLite 버퍼에 커밋합니다."""
import argparse
import time
import math
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
from shared_fleet_telemetry_policy import BUCKET_SECONDS, BUFFER_MESSAGES_PER_VEHICLE, SignalCollector

from shared_runtime_config import (
    VEHICLE_TELEMETRY_CONFIG, add_env_argument, load_vehicle_telemetry_config, project_path,
)

import traci


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUMO_CONFIG = PROJECT_ROOT / "scenario" / "gangnam" / "osm.sumocfg"

STEP_SECONDS = BUCKET_SECONDS


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="SUMO → 차량 SQLite 버퍼 수집")
    add_env_argument(parser, "--vehicle-telemetry-config", env="VEHICLE_FLEET_TELEMETRY_CONFIG_PATH",
                     default=VEHICLE_TELEMETRY_CONFIG, type=project_path)
    add_env_argument(parser, "--vehicle-buffer-db", env="VEHICLE_SQLITE_BUFFER_DB_PATH",
                     default=PROJECT_ROOT / "data" / "event_buffer.db", type=project_path)
    add_env_argument(parser, "--vehicle-buffer-max-messages", env="VEHICLE_BUFFER_MAX_MESSAGES",
                     default=BUFFER_MESSAGES_PER_VEHICLE, type=int)
    add_env_argument(parser, "--sumo-config", env="VEHICLE_SUMO_CONFIG_PATH",
                     default=SUMO_CONFIG, type=project_path)
    add_env_argument(parser, "--sumo-binary", env="VEHICLE_SUMO_BINARY", default="sumo-gui")
    parser.add_argument("--sumo-end-seconds", type=float, default=None,
                        help="검증용 시뮬레이션 종료 시각; 생략하면 전체 시나리오")
    args = parser.parse_args(argv)
    if args.sumo_end_seconds is not None and (not math.isfinite(args.sumo_end_seconds) or args.sumo_end_seconds <= 0):
        parser.error("Require positive finite --sumo-end-seconds")
    if args.vehicle_buffer_max_messages < 1:
        parser.error("VEHICLE_BUFFER_MAX_MESSAGES must be positive")
    try:
        args.signal_interval_seconds = load_vehicle_telemetry_config(args.vehicle_telemetry_config)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    run_id = str(uuid4())
    vehicle_sequences: dict[str, int] = {}
    collector = SignalCollector(args.signal_interval_seconds)

    # 1. SQLite 버퍼 연결
    buffer = VehicleSQLiteBuffer(
        args.vehicle_buffer_db,
        max_events=args.vehicle_buffer_max_messages, per_vehicle=True,
    )
    sumo_started = False

    try:
        # Kafka 상태와 관계없이 SUMO 수집을 시작합니다.
        traci.start([
            args.sumo_binary,
            "-c",
            str(args.sumo_config),
            "--step-length",
            str(STEP_SECONDS),
        ])
        sumo_started = True

        while (traci.simulation.getMinExpectedNumber() > 0
               and (args.sumo_end_seconds is None
                    or traci.simulation.getTime() < args.sumo_end_seconds)):
            step_started_at = time.monotonic()

            # 0.5초 묶음마다 선택한 차량 신호를 관찰합니다.
            traci.simulationStep()

            simulation_time = traci.simulation.getTime()
            vehicle_ids = traci.vehicle.getIDList()

            stored_count = 0
            for vehicle_id in vehicle_ids:
                x, y = traci.vehicle.getPosition(vehicle_id)
                longitude, latitude = traci.simulation.convertGeo(x, y)
                selected = collector.collect(vehicle_id, {
                    "location": {"latitude": latitude, "longitude": longitude},
                    "speed_mps": traci.vehicle.getSpeed(vehicle_id),
                }, simulation_time)
                if not selected:
                    continue
                sequence_no = vehicle_sequences.get(vehicle_id, 0) + 1

                event_time = datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                ).replace("+00:00", "Z")

                event = {
                    "run_id": run_id,
                    "event_id": f"{run_id}:{vehicle_id}:{sequence_no}",
                    "vehicle_id": vehicle_id,
                    "sequence_no" : sequence_no,
                    "event_time" : event_time,
                    "simulation_time": simulation_time,
                    "signals": selected,
                }
                buffer.save(event)
                collector.committed(vehicle_id, selected, simulation_time)
                vehicle_sequences[vehicle_id] = sequence_no
                stored_count += 1

            print(
                f"time={simulation_time:>6.1f}s | "
                f"active={len(vehicle_ids):>4} | "
                f"stored={stored_count:>4}"
            )

            # 시뮬레이션 속도를 실제 시간과 맞춥니다.
            elapsed = time.monotonic() - step_started_at
            time.sleep(max(0, STEP_SECONDS - elapsed))

    finally:
        try:
            if sumo_started:
                traci.close()
        finally:
            buffer.close()

if __name__ == "__main__":
    main()

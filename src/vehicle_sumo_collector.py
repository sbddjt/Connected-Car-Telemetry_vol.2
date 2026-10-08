"""차량 역할: SUMO 신호를 선택해 차량 SQLite 버퍼에 커밋합니다."""
import argparse
import time
import math
from collections import deque
from shared_pipeline_metrics import PipelineMetrics
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4
from vehicle_sqlite_buffer import VehicleSQLiteBuffer, BufferCapacityExceeded
from vehicle_sharded_buffer import ShardedVehicleSQLiteBuffer
from shared_fleet_telemetry_policy import BUCKET_SECONDS, BUFFER_MESSAGES_PER_VEHICLE, SignalCollector

from shared_runtime_config import (
    VEHICLE_TELEMETRY_CONFIG, add_env_argument, load_vehicle_telemetry_config, project_path,
)

import traci


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUMO_CONFIG = PROJECT_ROOT / "config" / "gangnam_expanded_sumo.sumocfg"

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
    parser.add_argument("--sumo-max-vehicles", type=int, default=None,
                        help="Override SUMO concurrent vehicle limit for a small deployment check")
    parser.add_argument("--sumo-end-seconds", type=float, default=None,
                        help="검증용 시뮬레이션 종료 시각; 생략하면 전체 시나리오")
    add_env_argument(parser, "--vehicle-buffer-directory", env="VEHICLE_SQLITE_BUFFER_DIRECTORY", default=None, type=lambda value: project_path(value) if value else None)
    add_env_argument(parser, "--vehicle-buffer-shards", env="VEHICLE_BUFFER_SHARDS", default=8, type=int)
    add_env_argument(parser, "--vehicle-buffer-overflow-policy", env="VEHICLE_BUFFER_OVERFLOW_POLICY",
                     default="drop_oldest")
    args = parser.parse_args(argv)
    if args.vehicle_buffer_overflow_policy not in {"drop_oldest", "block"}:
        parser.error("vehicle-buffer-overflow-policy supports drop_oldest or block")
    if args.vehicle_buffer_overflow_policy == "block" and args.vehicle_buffer_directory:
        parser.error("block policy requires a single vehicle-buffer-db; shard transactions are independent")
    if not 1 <= args.vehicle_buffer_shards <= 64:
        parser.error("Require 1 <= vehicle-buffer-shards <= 64")
    if args.sumo_end_seconds is not None and (not math.isfinite(args.sumo_end_seconds) or args.sumo_end_seconds <= 0):
        parser.error("Require positive finite --sumo-end-seconds")
    if args.sumo_max_vehicles is not None and args.sumo_max_vehicles < 1:
        parser.error("Require positive --sumo-max-vehicles")
    if args.vehicle_buffer_max_messages < 1:
        parser.error("VEHICLE_BUFFER_MAX_MESSAGES must be positive")
    try:
        args.signal_interval_seconds = load_vehicle_telemetry_config(args.vehicle_telemetry_config)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return args


def save_collected_batch(buffer, batch, retry_seconds=1.0):
    """Freeze this simulation step while the sender makes room; retry identical IDs."""
    while True:
        try:
            buffer.save_many(batch)
            return
        except BufferCapacityExceeded:
            print("[버퍼 대기] 미확인 기록 보존; SUMO 진행을 멈추고 ACK 삭제를 기다립니다.")
            time.sleep(retry_seconds)


def main(argv=None) -> None:
    args = parse_args(argv)
    run_id = str(uuid4())
    vehicle_sequences: dict[str, int] = {}
    collector = SignalCollector(args.signal_interval_seconds)

    # 1. SQLite 버퍼 연결
    buffer = (ShardedVehicleSQLiteBuffer(args.vehicle_buffer_directory, args.vehicle_buffer_shards,
                                         max_events=args.vehicle_buffer_max_messages)
              if args.vehicle_buffer_directory else VehicleSQLiteBuffer(
                  args.vehicle_buffer_db, max_events=args.vehicle_buffer_max_messages, per_vehicle=True,
                  overflow_policy=args.vehicle_buffer_overflow_policy))
    sumo_started = False
    metrics = PipelineMetrics("vehicle-collector")
    total_stored = total_departed = total_arrived = 0
    step_durations = deque(maxlen=120)

    try:
        # Kafka 상태와 관계없이 SUMO 수집을 시작합니다.
        sumo_command = [args.sumo_binary, "-c", str(args.sumo_config),
                        "--step-length", str(STEP_SECONDS)]
        if args.sumo_max_vehicles is not None:
            sumo_command += ["--max-num-vehicles", str(args.sumo_max_vehicles)]
        traci.start(sumo_command)
        sumo_started = True

        while (traci.simulation.getMinExpectedNumber() > 0
               and (args.sumo_end_seconds is None
                    or traci.simulation.getTime() < args.sumo_end_seconds)):
            step_started_at = time.monotonic()

            # 0.5초 묶음마다 선택한 차량 신호를 관찰합니다.
            traci.simulationStep()

            simulation_time = traci.simulation.getTime()
            total_departed += len(traci.simulation.getDepartedIDList())
            total_arrived += len(traci.simulation.getArrivedIDList())
            vehicle_ids = traci.vehicle.getIDList()

            stored_count = 0
            batch = []
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
                batch.append(event)

            # 모든 차량의 선택 기록을 함께 커밋한 뒤 수집 상태를 갱신합니다.
            save_collected_batch(buffer, batch)
            for event in batch:
                collector.committed(event["vehicle_id"], event["signals"], simulation_time)
                vehicle_sequences[event["vehicle_id"]] = event["sequence_no"]
            stored_count = len(batch)

            print(
                f"time={simulation_time:>6.1f}s | "
                f"active={len(vehicle_ids):>4} | "
                f"stored={stored_count:>4}"
            )

            # 시뮬레이션 속도를 실제 시간과 맞춥니다.
            elapsed = time.monotonic() - step_started_at
            step_durations.append(elapsed)
            total_stored += stored_count
            ordered = sorted(step_durations)
            metrics.publish(total_stored, run_id=run_id, simulation_time=simulation_time,
                            active_vehicles=len(vehicle_ids), departed_vehicles=total_departed,
                            buffer_shards=args.vehicle_buffer_shards if args.vehicle_buffer_directory else 1,
                            arrived_vehicles=total_arrived, collection_step_seconds=round(elapsed, 3),
                            collection_step_p95_seconds=round(ordered[int((len(ordered)-1)*.95)], 3))
            time.sleep(max(0, STEP_SECONDS - elapsed))

    finally:
        try:
            if sumo_started:
                metrics.publish(total_stored, force=True, run_id=run_id, active_vehicles=0,
                                simulation_time=traci.simulation.getTime(), departed_vehicles=total_departed,
                                arrived_vehicles=total_arrived, stopped=True)
                traci.close()
        finally:
            buffer.close()

if __name__ == "__main__":
    main()

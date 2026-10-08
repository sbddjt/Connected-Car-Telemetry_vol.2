"""Container role selection and SIGTERM cleanup for the existing programs."""
import os
from pathlib import Path
import runpy
import signal
import sys
import time

ROLES = {
    "receiver": "server_fleet_telemetry.py",
    "history-consumer": "server_telemetry_consumer.py",
    "redis-consumer": "server_redis_projection_consumer.py",
    "query-api": "server_vehicle_query_api.py",
    "vehicle-sender": "vehicle_fleet_telemetry_client.py",
    "vehicle-collector": "vehicle_sumo_collector.py",
}

def terminate(signum, frame):
    raise KeyboardInterrupt

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ROLES:
        raise SystemExit("Choose a role: " + ", ".join(ROLES))
    role = sys.argv[1]
    script = Path(__file__).parent / ROLES[role]
    sys.argv = [str(script), *sys.argv[2:]]
    signal.signal(signal.SIGTERM, terminate)
    try:
        runpy.run_path(str(script), run_name="__main__")
        # A finite scenario must not restart automatically while the sender drains.
        if role == "vehicle-collector" and os.environ.get("VEHICLE_IDLE_AFTER_RUN") == "1":
            print("[시뮬레이션 완료] 전송기는 남은 SQLite 기록을 계속 전송합니다.", flush=True)
            while True:
                time.sleep(60)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

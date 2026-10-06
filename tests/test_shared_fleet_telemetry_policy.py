import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shared_fleet_telemetry_policy import Backoff, SignalCollector, validate_event
from vehicle_sqlite_buffer import VehicleSQLiteBuffer
import vehicle_sumo_collector
from test_server_kafka_dispatcher import event


class SignalPolicyTests(unittest.TestCase):
    def test_initial_changed_and_deferred_latest_value(self):
        collector = SignalCollector({"speed_mps": 1.0})
        selected = collector.collect("car", {"speed_mps": 0.0}, 0.5)
        self.assertEqual(selected, {"speed_mps": 0.0})
        collector.committed("car", selected, 0.5)
        self.assertEqual(collector.collect("car", {"speed_mps": 1.0}, 1.0), {})
        # 간격이 지난 시점의 최신 2.0만 기록하며 중간 1.0은 수집 대상으로 선택하지 않습니다.
        self.assertEqual(collector.collect("car", {"speed_mps": 2.0}, 1.5), {"speed_mps": 2.0})
        collector.committed("car", {"speed_mps": 2.0}, 1.5)
        self.assertEqual(collector.collect("car", {"speed_mps": 2.0}, 3.0), {})

    def test_signal_and_vehicle_intervals_are_independent(self):
        collector = SignalCollector({"location": 1, "speed_mps": 2})
        initial = {"location": {"latitude": 37, "longitude": 127}, "speed_mps": 0}
        collector.committed("a", collector.collect("a", initial, 0), 0)
        changed = {"location": {"latitude": 38, "longitude": 127}, "speed_mps": 5}
        self.assertEqual(collector.collect("a", changed, 1), {"location": changed["location"]})
        self.assertEqual(collector.collect("b", changed, 1), changed)

    def test_failed_commit_does_not_advance_sampling_state(self):
        collector = SignalCollector()
        self.assertEqual(collector.collect("car", {"speed_mps": 5}, 0), {"speed_mps": 5})
        # committed 호출이 없으면 다음 묶음에도 다시 선택할 수 있습니다.
        self.assertEqual(collector.collect("car", {"speed_mps": 5}, 0.5), {"speed_mps": 5})

    def test_backoff_caps_at_thirty_and_resets_on_ack(self):
        retry = Backoff()
        self.assertEqual([retry.fail(100) for _ in range(7)], [1, 2, 4, 8, 16, 30, 30])
        retry.reset()
        self.assertEqual(retry.fail(200), 1)

    def test_invalid_policy_and_signal_data_rejected(self):
        for intervals in ({}, {"unknown": 1}, {"speed_mps": 0}, {"speed_mps": float("nan")}):
            with self.assertRaises(ValueError):
                SignalCollector(intervals)
        for signals in ({}, {"speed_mps": float("nan")}, {"speed_mps": -1},
                        {"location": {"latitude": 91, "longitude": 127}}, {"unknown": 1}):
            with self.assertRaises(ValueError):
                validate_event(event(signals=signals))
        for settings in ({"initial": 0}, {"maximum": float("inf")}, {"initial": 31}):
            with self.assertRaises(ValueError):
                Backoff(**settings)

    def test_capacity_is_per_vehicle_not_for_the_whole_sumo_fleet(self):
        with tempfile.TemporaryDirectory() as directory:
            buffer = VehicleSQLiteBuffer(Path(directory) / "vehicle.db", max_events=2)
            try:
                for car in ("a", "b"):
                    for number in (1, 2, 3):
                        buffer.save(event(number, vehicle=car))
                self.assertEqual(len(buffer.get_pending()), 4)
                for car in ("a", "b"):
                    self.assertEqual(
                        [e["sequence_no"] for _, e in buffer.get_pending_records(vehicle_id=car)], [2, 3]
                    )
                self.assertEqual(buffer.connection.execute(
                    "SELECT value FROM buffer_stats WHERE name='capacity_dropped'"
                ).fetchone()[0], 2)
            finally:
                buffer.close()

    def test_sumo_half_second_buckets_save_only_selected_signals_without_kafka(self):
        with tempfile.TemporaryDirectory() as directory:
            state = {"step": 0, "closed": False}
            simulation = SimpleNamespace(
                getMinExpectedNumber=lambda: int(state["step"] < 3),
                getTime=lambda: state["step"] * 0.5,
                convertGeo=lambda x, y: (127.0, 37.0 + state["step"] / 100),
            )
            vehicle = SimpleNamespace(
                getIDList=lambda: ("a", "b"), getPosition=lambda car: (0, 0),
                getSpeed=lambda car: 10 if state["step"] == 1 else 20,
            )
            with (
                patch.object(vehicle_sumo_collector, "PROJECT_ROOT", Path(directory)),
                patch.object(vehicle_sumo_collector.traci, "start") as start,
                patch.object(vehicle_sumo_collector.traci, "simulation", simulation),
                patch.object(vehicle_sumo_collector.traci, "vehicle", vehicle),
                patch.object(vehicle_sumo_collector.traci, "simulationStep", lambda: state.update(step=state["step"] + 1)),
                patch.object(vehicle_sumo_collector.traci, "close", lambda: state.update(closed=True)),
                patch.object(vehicle_sumo_collector.time, "sleep"),
            ):
                vehicle_sumo_collector.main([])
            self.assertIn("0.5", start.call_args.args[0])
            buffer = VehicleSQLiteBuffer(Path(directory) / "data" / "event_buffer.db")
            try:
                records = buffer.get_pending()
                self.assertEqual(len(records), 4)
                self.assertEqual([e["sequence_no"] for e in records], [1, 1, 2, 2])
                self.assertEqual([e["signals"]["speed_mps"] for e in records], [10, 10, 20, 20])
                self.assertEqual(state["step"], 3)
                self.assertTrue(state["closed"])
            finally:
                buffer.close()


if __name__ == "__main__":
    unittest.main()

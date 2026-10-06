import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import server_fleet_telemetry
import server_telemetry_consumer
import vehicle_fleet_telemetry_client
import vehicle_sumo_collector
from shared_runtime_config import (
    PROJECT_ROOT, load_server_telemetry_config, load_vehicle_telemetry_config,
)
from vehicle_sqlite_buffer import VehicleSQLiteBuffer


class RuntimeConfigTests(unittest.TestCase):
    def setUp(self):
        self.environ = patch.dict(os.environ, {}, clear=True)
        self.environ.start()
        self.addCleanup(self.environ.stop)

    def write_config(self, directory, name, config):
        path = Path(directory) / name
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    def test_vehicle_processes_share_buffer_path_and_capacity_environment(self):
        with patch.dict(os.environ, {
            "VEHICLE_SQLITE_BUFFER_DB_PATH": "data/custom_vehicle.db",
            "VEHICLE_BUFFER_MAX_MESSAGES": "42",
            "SERVER_SQLITE_STORAGE_DB_PATH": "data/custom_server.db",
        }):
            collector = vehicle_sumo_collector.parse_args([])
            client = vehicle_fleet_telemetry_client.parse_args([])
            consumer = server_telemetry_consumer.parse_args([])
        self.assertEqual(collector.vehicle_buffer_db, PROJECT_ROOT / "data/custom_vehicle.db")
        self.assertEqual(client.vehicle_buffer_db, collector.vehicle_buffer_db)
        self.assertEqual(client.vehicle_buffer_max_messages, 42)
        self.assertEqual(collector.vehicle_buffer_max_messages, 42)
        self.assertEqual(consumer.server_storage_db, PROJECT_ROOT / "data/custom_server.db")

    def test_cli_overrides_invalid_numeric_environment_before_conversion(self):
        with patch.dict(os.environ, {
            "VEHICLE_MAX_IN_FLIGHT_MESSAGES": "invalid",
            "VEHICLE_FLEET_TELEMETRY_SERVER_URL": "ws://environment:8765",
        }):
            client = vehicle_fleet_telemetry_client.parse_args([
                "--vehicle-max-in-flight", "7",
                "--fleet-telemetry-server-url", "ws://command:9000",
            ])
        self.assertEqual(client.max_in_flight, 7)
        self.assertEqual(client.fleet_telemetry_server_url, "ws://command:9000")

    def test_server_config_environment_and_cli_are_shared_by_producer_and_consumer(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, "server.json", {
                "host": "localhost", "port": 9876,
                "reliable_ack_sources": {"V": "kafka"},
                "kafka": {"bootstrap.servers": "configured:9092", "message.timeout.ms": 1234},
            })
            with patch.dict(os.environ, {
                "SERVER_FLEET_TELEMETRY_CONFIG_PATH": str(config),
                "SERVER_KAFKA_BOOTSTRAP_SERVERS": "environment:9092",
                "SERVER_KAFKA_VEHICLE_TOPIC": "custom-vehicle-topic",
            }):
                receiver = server_fleet_telemetry.parse_args([])
                consumer = server_telemetry_consumer.parse_args([])
                overridden = server_fleet_telemetry.parse_args([
                    "--kafka-bootstrap-servers", "command:9092", "--server-port", "7654",
                ])
            self.assertEqual(receiver.server_port, 9876)
            self.assertEqual(receiver.kafka_message_timeout_ms, 1234)
            self.assertEqual(receiver.kafka_bootstrap_servers, "environment:9092")
            self.assertEqual(consumer.kafka_bootstrap_servers, receiver.kafka_bootstrap_servers)
            self.assertEqual(consumer.kafka_vehicle_topic, receiver.kafka_vehicle_topic)
            self.assertEqual(receiver.kafka_vehicle_topic, "custom-vehicle-topic")
            self.assertEqual(overridden.kafka_bootstrap_servers, "command:9092")
            self.assertEqual(overridden.server_port, 7654)

    def test_unsupported_vehicle_policies_are_rejected_before_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            for config in (
                {"delivery_policy": "drop", "fields": {"VehicleSpeed": {"interval_seconds": 1}}},
                {"delivery_policy": "latest", "fields": {"Unknown": {"interval_seconds": 1}}},
                {"delivery_policy": "latest", "fields": {"Location": {"interval_seconds": True}}},
                {"delivery_policy": "latest", "fields": {"Location": {
                    "interval_seconds": 1, "minimum_delta": 1,
                }}},
            ):
                with self.subTest(config=config):
                    path = self.write_config(directory, "vehicle.json", config)
                    with self.assertRaises(ValueError):
                        load_vehicle_telemetry_config(path)

    def test_kafka_success_ack_cannot_be_silently_disabled_in_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_config(directory, "server.json", {
                "host": "localhost", "port": 8765,
                "reliable_ack_sources": {"V": "logger"},
                "kafka": {"bootstrap.servers": "localhost:9092", "message.timeout.ms": 30000},
            })
            with self.assertRaises(ValueError):
                load_server_telemetry_config(path)
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                server_fleet_telemetry.parse_args(["--server-telemetry-config", str(path)])
            self.assertEqual(error.exception.code, 2)

    def test_sumo_uses_selected_field_interval_and_environment_buffer_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, "vehicle.json", {
                "delivery_policy": "latest",
                "fields": {"VehicleSpeed": {"interval_seconds": 1.5}},
            })
            database = Path(directory) / "onboard.db"
            state = {"step": 0}
            simulation = SimpleNamespace(
                getMinExpectedNumber=lambda: int(state["step"] < 4),
                getTime=lambda: state["step"] * 0.5,
                getDepartedIDList=lambda: (), getArrivedIDList=lambda: (),
                convertGeo=lambda x, y: (127.0, 37.0),
            )
            vehicle = SimpleNamespace(
                getIDList=lambda: ("car",), getPosition=lambda car: (0, 0),
                getSpeed=lambda car: float(state["step"]),
            )
            with (
                patch.dict(os.environ, {
                    "VEHICLE_FLEET_TELEMETRY_CONFIG_PATH": str(config),
                    "VEHICLE_SQLITE_BUFFER_DB_PATH": str(database),
                    "VEHICLE_BUFFER_MAX_MESSAGES": "1",
                }),
                patch.object(vehicle_sumo_collector, "PipelineMetrics"),
                patch.object(vehicle_sumo_collector.traci, "start"),
                patch.object(vehicle_sumo_collector.traci, "simulation", simulation),
                patch.object(vehicle_sumo_collector.traci, "vehicle", vehicle),
                patch.object(vehicle_sumo_collector.traci, "simulationStep",
                             lambda: state.update(step=state["step"] + 1)),
                patch.object(vehicle_sumo_collector.traci, "close"),
                patch.object(vehicle_sumo_collector.time, "sleep"),
            ):
                vehicle_sumo_collector.main([])
            buffer = VehicleSQLiteBuffer(database)
            try:
                records = buffer.get_pending()
                # Selected at 0.5 and 2.0s; the configured one-message capacity keeps the last.
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["sequence_no"], 2)
                self.assertEqual(records[0]["simulation_time"], 2.0)
                self.assertEqual(records[0]["signals"], {"speed_mps": 4.0})
                self.assertEqual(buffer.connection.execute(
                    "SELECT value FROM buffer_stats WHERE name='capacity_dropped'"
                ).fetchone()[0], 1)
            finally:
                buffer.close()

    def test_legacy_cli_aliases_resolve_to_clear_named_options(self):
        old = vehicle_fleet_telemetry_client.parse_args([
            "--db-path", "data/alias.db", "--server-url", "ws://localhost:9999",
        ])
        new = vehicle_fleet_telemetry_client.parse_args([
            "--vehicle-buffer-db", "data/alias.db",
            "--fleet-telemetry-server-url", "ws://localhost:9999",
        ])
        self.assertEqual(old.vehicle_buffer_db, new.vehicle_buffer_db)
        self.assertEqual(old.fleet_telemetry_server_url, new.fleet_telemetry_server_url)


if __name__ == "__main__":
    unittest.main()

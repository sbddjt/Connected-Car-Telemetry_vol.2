import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from render_kubernetes import build_objects

class KubernetesConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.objects = [item for group in build_objects("example/telemetry:v1", "example/telemetry:v1-simulator").values() for item in group]

    def find(self, kind, name):
        return next(item for item in self.objects if item["kind"] == kind and item["metadata"]["name"] == name)

    def test_stateful_data_survives_scale_down_and_workload_deletion(self):
        for name in ("kafka", "mongodb", "redis", "vehicle-simulator"):
            spec = self.find("StatefulSet", name)["spec"]
            self.assertEqual(spec["persistentVolumeClaimRetentionPolicy"], {"whenDeleted": "Retain", "whenScaled": "Retain"})
            self.assertEqual(spec["volumeClaimTemplates"][0]["metadata"]["name"], "data")

    def test_kafka_replication_and_unbounded_topic_retention(self):
        spec = self.find("StatefulSet", "kafka")["spec"]
        self.assertEqual(spec["replicas"], 3)
        settings = {entry["name"]: entry["value"] for entry in spec["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(settings["KAFKA_MIN_INSYNC_REPLICAS"], "2")
        self.assertEqual(settings["KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE"], "false")
        command = self.find("Job", "kafka-topics")["spec"]["template"]["spec"]["containers"][0]["args"][0]
        self.assertIn("retention.ms=-1", command)
        self.assertIn("retention.bytes=-1", command)
        self.assertIn("--replication-factor 3", command)

    def test_collector_and_sender_share_one_durable_buffer_without_drops(self):
        spec = self.find("StatefulSet", "vehicle-simulator")["spec"]
        self.assertEqual(spec["replicas"], 0)
        containers = spec["template"]["spec"]["containers"]
        self.assertEqual(len(containers), 2)
        for container in containers:
            settings = {entry["name"]: entry["value"] for entry in container["env"]}
            self.assertEqual(settings["VEHICLE_BUFFER_OVERFLOW_POLICY"], "block")
            self.assertEqual(settings["VEHICLE_SQLITE_BUFFER_DB_PATH"], "/app/data/vehicle/event_buffer.db")
            self.assertEqual(container["volumeMounts"], [{"name": "data", "mountPath": "/app/data"}])

    def test_laptop_profile_is_consistent_single_broker_and_five_vehicles(self):
        groups = build_objects(profile="laptop")
        kafka = next(item for item in groups["kafka.yaml"] if item["kind"] == "StatefulSet")
        self.assertEqual(kafka["spec"]["replicas"], 1)
        settings = {entry["name"]: entry["value"] for entry in kafka["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(settings["KAFKA_MIN_INSYNC_REPLICAS"], "1")
        self.assertEqual(settings["KAFKA_CONTROLLER_QUORUM_VOTERS"], "1@kafka-0.kafka:9093")
        command = groups["topics.yaml"][0]["spec"]["template"]["spec"]["containers"][0]["args"][0]
        self.assertIn("--replication-factor 1", command)
        self.assertNotIn("kafka-1", command)
        self.assertIn("retention.ms=-1", command)
        simulator = next(item for item in groups["simulator.yaml"] if item["kind"] == "StatefulSet")
        self.assertEqual(simulator["spec"]["template"]["spec"]["containers"][0]["args"][-1], "5")

    def test_query_liveness_does_not_restart_on_redis_outage(self):
        container = self.find("Deployment", "query-api")["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["readinessProbe"]["httpGet"]["path"], "/api/health")
        self.assertEqual(container["livenessProbe"]["httpGet"]["path"], "/")

if __name__ == "__main__":
    unittest.main()

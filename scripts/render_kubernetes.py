"""Render the vol.2 learning cluster without requiring Helm or a YAML dependency."""
import argparse
import json
from pathlib import Path

NAMESPACE = "telemetry-v2"
DEFAULT_IMAGE = "sbddjt/connected-car-telemetry-v2:v0.1.0"
DEFAULT_SIMULATOR_IMAGE = "sbddjt/connected-car-telemetry-v2:v0.1.0-simulator"

def env(mapping):
    return [{"name": name, "value": str(value)} for name, value in mapping.items()]

def resource(kind, name, spec=None, **extra):
    item = {"apiVersion": "apps/v1" if kind in {"Deployment", "StatefulSet"} else "v1",
            "kind": kind, "metadata": {"name": name, "namespace": NAMESPACE}}
    if spec is not None:
        item["spec"] = spec
    item.update(extra)
    return item

def service(name, port, *, headless=False, publish_unready=False):
    spec = {"selector": {"app": name}, "ports": [{"name": "tcp", "port": port, "targetPort": port}]}
    if headless:
        spec["clusterIP"] = "None"
    if publish_unready:
        spec["publishNotReadyAddresses"] = True
    return resource("Service", name, spec)

def volume_claim(name, size):
    return {"metadata": {"name": name}, "spec": {
        "accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": size}}}}

def stateful(name, container, size):
    return resource("StatefulSet", name, {
        "serviceName": name, "replicas": 1,
        "selector": {"matchLabels": {"app": name}},
        "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain", "whenScaled": "Retain"},
        "template": {"metadata": {"labels": {"app": name}}, "spec": {
            "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 60,
            "containers": [container]}},
        "volumeClaimTemplates": [volume_claim("data", size)]})

def server_deployment(name, role, image):
    container = {"name": name, "image": image, "imagePullPolicy": "IfNotPresent", "args": [role],
        "envFrom": [{"configMapRef": {"name": "telemetry-runtime"}}],
        "resources": {"requests": {"cpu": "50m", "memory": "96Mi"},
                      "limits": {"cpu": "1", "memory": "256Mi"}},
        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]}},
        "volumeMounts": [{"name": "runtime", "mountPath": "/app/data"}, {"name": "tmp", "mountPath": "/tmp"}]}
    if role == "receiver":
        container.update(ports=[{"containerPort": 8765}],
            readinessProbe={"tcpSocket": {"port": 8765}, "periodSeconds": 5},
            startupProbe={"tcpSocket": {"port": 8765}, "periodSeconds": 5, "failureThreshold": 24})
    if role == "query-api":
        container.update(ports=[{"containerPort": 8080}],
            readinessProbe={"httpGet": {"path": "/api/health", "port": 8080}, "periodSeconds": 5},
            livenessProbe={"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 10},
            startupProbe={"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 5, "failureThreshold": 24})
    return resource("Deployment", name, {
        "replicas": 1, "selector": {"matchLabels": {"app": name}},
        "template": {"metadata": {"labels": {"app": name}}, "spec": {
            "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 60,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001},
            "containers": [container], "volumes": [{"name": "runtime", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]}}})

def build_objects(image=DEFAULT_IMAGE, simulator_image=DEFAULT_SIMULATOR_IMAGE, profile="replicated"):
    if profile not in {"laptop", "replicated"}:
        raise ValueError("profile must be laptop or replicated")
    # Cluster-only plaintext endpoints. Do not expose this learning profile to the Internet.
    objects = {"namespace.yaml": [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}}]}
    objects["config.yaml"] = [resource("ConfigMap", "telemetry-runtime", data={
        "SERVER_FLEET_TELEMETRY_HOST": "0.0.0.0", "SERVER_QUERY_API_HOST": "0.0.0.0",
        "SERVER_KAFKA_BOOTSTRAP_SERVERS": "kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092",
        "SERVER_MONGODB_URI": "mongodb://mongodb:27017", "SERVER_REDIS_URL": "redis://redis:6379/0",
        "SHARED_PIPELINE_METRICS_DIRECTORY": "/app/data/runtime/metrics"})]

    kafka = {"name": "kafka", "image": "apache/kafka:4.3.1", "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/bash", "-ec"], "args": [
            'export KAFKA_NODE_ID="$((${HOSTNAME##*-}+1))"\n'
            'export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${HOSTNAME}.kafka:9092"\n'
            'exec /etc/kafka/docker/run'],
        "ports": [{"name": "broker", "containerPort": 9092}, {"name": "controller", "containerPort": 9093}],
        "env": env({"CLUSTER_ID": "4L6g3nShT-eMCtK--X86sw", "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka-0.kafka:9093,2@kafka-1.kafka:9093,3@kafka-2.kafka:9093",
            "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_DEFAULT_REPLICATION_FACTOR": 3, "KAFKA_MIN_INSYNC_REPLICAS": 2,
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 3, "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 3,
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 2, "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
            "KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE": "false", "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
            "KAFKA_LOG_RETENTION_HOURS": -1, "KAFKA_LOG_RETENTION_BYTES": -1,
            "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": 0}),
        "resources": {"requests": {"cpu": "100m", "memory": "640Mi"}, "limits": {"cpu": "1", "memory": "1Gi"}},
        "volumeMounts": [{"name": "data", "mountPath": "/var/lib/kafka"}],
        "readinessProbe": {"tcpSocket": {"port": 9092}, "periodSeconds": 5},
        "startupProbe": {"tcpSocket": {"port": 9092}, "periodSeconds": 5, "failureThreshold": 60}}
    kafka_set = stateful("kafka", kafka, "5Gi")
    kafka_set["spec"].update(replicas=3, podManagementPolicy="Parallel")
    kafka_set["spec"]["template"]["spec"]["securityContext"] = {"fsGroup": 1000}
    kafka_set["spec"]["template"]["spec"]["affinity"] = {"podAntiAffinity": {
        "preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 100, "podAffinityTerm": {
            "labelSelector": {"matchLabels": {"app": "kafka"}}, "topologyKey": "kubernetes.io/hostname"}}]}}
    objects["kafka.yaml"] = [service("kafka", 9092, headless=True, publish_unready=True), kafka_set]
    topic_command = '''set -eu
for topic in vehicle-telemetry-v1 vehicle-connectivity-v1 vehicle-errors-v1; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092 --create --if-not-exists --topic "$topic" --partitions 12 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=-1 --config retention.bytes=-1 --config cleanup.policy=delete
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092 --entity-type topics --entity-name "$topic" --alter --add-config min.insync.replicas=2,retention.ms=-1,retention.bytes=-1,cleanup.policy=delete
done
'''
    objects["topics.yaml"] = [{"apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": "kafka-topics", "namespace": NAMESPACE},
        "spec": {"backoffLimit": 12, "activeDeadlineSeconds": 900, "template": {"spec": {
            "automountServiceAccountToken": False, "restartPolicy": "OnFailure", "containers": [{
                "name": "create-topics", "image": "apache/kafka:4.3.1", "command": ["/bin/bash", "-ec"],
                "args": [topic_command], "env": env({"KAFKA_HEAP_OPTS": "-Xms128m -Xmx256m"}),
                "resources": {"requests": {"cpu": "50m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}}]}}}}]

    mongodb = {"name": "mongodb", "image": "mongo:8.0", "args": ["--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25"],
        "ports": [{"containerPort": 27017}], "volumeMounts": [{"name": "data", "mountPath": "/data/db"}],
        "resources": {"requests": {"cpu": "100m", "memory": "384Mi"}, "limits": {"cpu": "1", "memory": "768Mi"}},
        "readinessProbe": {"exec": {"command": ["mongosh", "--quiet", "--eval", "db.adminCommand({ping:1}).ok"]}, "timeoutSeconds": 5},
        "startupProbe": {"tcpSocket": {"port": 27017}, "periodSeconds": 5, "failureThreshold": 60}}
    redis = {"name": "redis", "image": "redis:8.2-alpine", "args": ["redis-server", "--appendonly", "yes",
        "--appendfsync", "always", "--maxmemory-policy", "noeviction"],
        "ports": [{"containerPort": 6379}], "volumeMounts": [{"name": "data", "mountPath": "/data"}],
        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"cpu": "1", "memory": "256Mi"}},
        "readinessProbe": {"exec": {"command": ["redis-cli", "ping"]}},
        "startupProbe": {"tcpSocket": {"port": 6379}, "periodSeconds": 5, "failureThreshold": 60}}
    objects["mongodb.yaml"] = [service("mongodb", 27017, headless=True), stateful("mongodb", mongodb, "5Gi")]
    objects["redis.yaml"] = [service("redis", 6379, headless=True), stateful("redis", redis, "1Gi")]
    objects["servers.yaml"] = [server_deployment(name, role, image) for name, role in (
        ("receiver", "receiver"), ("history-consumer", "history-consumer"),
        ("redis-consumer", "redis-consumer"), ("query-api", "query-api"))]
    objects["servers.yaml"] += [service("receiver", 8765), service("query-api", 8080)]

    simulator_env = {"VEHICLE_SQLITE_BUFFER_DB_PATH": "/app/data/vehicle/event_buffer.db",
        "VEHICLE_BUFFER_OVERFLOW_POLICY": "block", "VEHICLE_FLEET_TELEMETRY_SERVER_URL": "ws://receiver:8765",
        "VEHICLE_MAX_IN_FLIGHT_MESSAGES": "2", "VEHICLE_SENDER_MAX_WORKERS": "32",
        "VEHICLE_SUMO_BINARY": "sumo", "VEHICLE_IDLE_AFTER_RUN": "1"}
    containers = []
    for name, role, role_image, arguments in (
        ("collector", "vehicle-collector", simulator_image,
         ["--sumo-end-seconds", "120", "--sumo-max-vehicles", "20"]),
        ("sender", "vehicle-sender", image, [])):
        containers.append({"name": name, "image": role_image, "args": [role, *arguments],
            "env": env(simulator_env), "volumeMounts": [{"name": "data", "mountPath": "/app/data"}],
            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
            "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}})
    simulator_set = stateful("vehicle-simulator", containers[0], "2Gi")
    simulator_set["spec"]["replicas"] = 0  # Explicitly start only after infrastructure is ready.
    simulator_set["spec"]["template"]["spec"].update(containers=containers, securityContext={
        "runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001})
    objects["simulator.yaml"] = [service("vehicle-simulator", 8765, headless=True), simulator_set]
    if profile == "laptop":
        # Only a new learning cluster: never downsize existing replicated broker data.
        runtime = objects["config.yaml"][0]["data"]
        runtime["SERVER_KAFKA_BOOTSTRAP_SERVERS"] = "kafka-0.kafka:9092"
        kafka_set["spec"]["replicas"] = 1
        changes = {
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka-0.kafka:9093",
            "KAFKA_DEFAULT_REPLICATION_FACTOR": "1", "KAFKA_MIN_INSYNC_REPLICAS": "1",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR": "1", "KAFKA_HEAP_OPTS": "-Xms128m -Xmx256m",
        }
        existing = {entry["name"]: entry for entry in kafka["env"]}
        for name, value in changes.items():
            if name in existing:
                existing[name]["value"] = value
            else:
                kafka["env"].append({"name": name, "value": value})
        kafka["resources"] = {"requests": {"cpu": "100m", "memory": "320Mi"},
                              "limits": {"cpu": "1", "memory": "512Mi"}}
        job = objects["topics.yaml"][0]["spec"]["template"]["spec"]["containers"][0]
        job["args"][0] = job["args"][0].replace(
            "kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092", "kafka-0.kafka:9092").replace(
            "--replication-factor 3", "--replication-factor 1").replace("min.insync.replicas=2", "min.insync.replicas=1")
        mongodb["resources"]["requests"]["memory"] = "256Mi"
        mongodb["resources"]["limits"]["memory"] = "512Mi"
        redis["resources"]["limits"]["memory"] = "128Mi"
        for item in objects["servers.yaml"]:
            if item["kind"] == "Deployment":
                container = item["spec"]["template"]["spec"]["containers"][0]
                container["resources"]["requests"]["memory"] = "32Mi"
                container["resources"]["limits"]["memory"] = "128Mi"
        for container in containers:
            container["resources"]["requests"]["memory"] = "64Mi"
            container["resources"]["limits"]["memory"] = "256Mi"
        containers[0]["args"][-1] = "5"
        containers[0]["args"][2] = "60"
    return objects

def yaml_lines(value, indent=0):
    """Emit plain mappings/sequences with JSON-quoted scalars (valid YAML 1.2)."""
    prefix = " " * indent
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, (dict, list)) and child:
                yield f"{prefix}{key}:"
                yield from yaml_lines(child, indent + 2)
            else:
                yield f"{prefix}{key}: {json.dumps(child, ensure_ascii=False)}"
    elif isinstance(value, list):
        for child in value:
            if isinstance(child, (dict, list)) and child:
                yield prefix + "-"
                yield from yaml_lines(child, indent + 2)
            else:
                yield prefix + "- " + json.dumps(child, ensure_ascii=False)

def render(output, image=DEFAULT_IMAGE, simulator_image=DEFAULT_SIMULATOR_IMAGE, profile="replicated"):
    output.mkdir(parents=True, exist_ok=True)
    objects = build_objects(image, simulator_image, profile)
    for name, documents in objects.items():
        output.joinpath(name).write_text("\n---\n".join("\n".join(yaml_lines(item)) for item in documents) + "\n", encoding="utf-8")
    kustomization = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                     "resources": list(objects)}
    output.joinpath("kustomization.yaml").write_text("\n".join(yaml_lines(kustomization)) + "\n", encoding="utf-8")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--simulator-image", default=DEFAULT_SIMULATOR_IMAGE)
    parser.add_argument("--profile", choices=("laptop", "replicated"), default="replicated")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parents[1] / "deploy" / "kubernetes")
    args = parser.parse_args()
    render(args.output, args.image, args.simulator_image, args.profile)
    print(f"Rendered Kubernetes resources: {args.output}")

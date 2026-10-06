"""역할별 환경변수와 Tesla 공개 명칭을 사용한 로컬 설정 로더."""
import argparse
import json
import math
import os
from pathlib import Path
from shared_kafka import DEFAULT_KAFKA_TOPICS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VEHICLE_TELEMETRY_CONFIG = PROJECT_ROOT / "config" / "vehicle_fleet_telemetry_config.json"
SERVER_TELEMETRY_CONFIG = PROJECT_ROOT / "config" / "server_fleet_telemetry_config.json"
# 공개 필드명을 로컬 SUMO 신호에 연결합니다. 전송 JSON의 속도 단위는 m/s입니다.
TESLA_FIELD_TO_LOCAL_SIGNAL = {"Location": "location", "VehicleSpeed": "speed_mps"}


def project_path(value):
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def add_env_argument(parser, flag, *, env, default=None, type=str, aliases=(), dest=None):
    """CLI > 프로세스 환경변수 > 설정 파일/기본값. .env는 자동으로 읽지 않습니다."""
    parser.add_argument(
        flag, *aliases, dest=dest, type=type,
        default=os.environ.get(env, default),
        help=f"환경변수: {env}",
    )


def _read_config(path, allowed):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"{path}: configuration must be an object")
    unknown = set(config) - allowed
    if unknown:
        raise ValueError(f"{path}: unsupported settings: {sorted(unknown)}")
    return config


def load_vehicle_telemetry_config(path):
    config = _read_config(path, {"delivery_policy", "fields"})
    if config.get("delivery_policy") != "latest":
        raise ValueError("Only delivery_policy=latest (resend all unacknowledged records) is supported")
    fields = config.get("fields")
    if not isinstance(fields, dict) or not fields:
        raise ValueError("fields must be a nonempty object")
    intervals = {}
    for name, settings in fields.items():
        if name not in TESLA_FIELD_TO_LOCAL_SIGNAL:
            raise ValueError(f"Unsupported telemetry field: {name}")
        if not isinstance(settings, dict) or set(settings) != {"interval_seconds"}:
            raise ValueError(f"{name}: only interval_seconds is supported")
        interval = settings["interval_seconds"]
        if (isinstance(interval, bool) or not isinstance(interval, (int, float))
                or not math.isfinite(interval) or interval <= 0):
            raise ValueError(f"{name}.interval_seconds must be positive and finite")
        intervals[TESLA_FIELD_TO_LOCAL_SIGNAL[name]] = interval
    return intervals


def load_server_telemetry_config(path):
    config = _read_config(path, {"host", "port", "reliable_ack_sources", "kafka", "tls", "kafka_topics", "redis"})
    # 이 실험은 Kafka 성공 후 ACK 정책만 지원합니다. 설정을 무시해 조기 ACK하지 않습니다.
    if config.get("reliable_ack_sources") != {"V": "kafka"}:
        raise ValueError('reliable_ack_sources must be {"V": "kafka"}')
    if not isinstance(config.get("host"), str) or not config["host"]:
        raise ValueError("host must be a nonempty string")
    port = config.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    kafka = config.get("kafka")
    if not isinstance(kafka, dict) or set(kafka) != {"bootstrap.servers", "message.timeout.ms"}:
        raise ValueError("kafka requires bootstrap.servers and message.timeout.ms")
    if not isinstance(kafka["bootstrap.servers"], str) or not kafka["bootstrap.servers"]:
        raise ValueError("kafka.bootstrap.servers must be a nonempty string")
    timeout = kafka["message.timeout.ms"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("kafka.message.timeout.ms must be positive")
    tls = config.get("tls", {})
    if (not isinstance(tls, dict) or set(tls) - {"server_cert", "server_key", "ca_file"}
            or any(not isinstance(value, str) or not value for value in tls.values())):
        raise ValueError("tls supports server_cert, server_key and ca_file paths")
    topics = config.setdefault("kafka_topics", dict(DEFAULT_KAFKA_TOPICS))
    if (not isinstance(topics, dict) or set(topics) != set(DEFAULT_KAFKA_TOPICS)
            or any(not isinstance(name, str) or not name for name in topics.values())
            or len(set(topics.values())) != len(topics)):
        raise ValueError("kafka_topics requires distinct topics for V, connectivity and errors")
    settings = config.setdefault("redis", {
        "addrs": ["127.0.0.1:6380"], "db": 0,
    })
    if (not isinstance(settings, dict)
            or set(settings) - {"addrs", "db", "username", "password", "tls"}):
        raise ValueError("Unsupported redis settings")
    addresses = settings.get("addrs", ["127.0.0.1:6380"])
    if (not isinstance(addresses, list) or len(addresses) != 1
            or not isinstance(addresses[0], str) or not addresses[0]):
        raise ValueError("This local implementation requires one standalone Redis address")
    database = settings.get("db", 0)
    if type(database) is not int or database < 0:
        raise ValueError("redis.db must be a nonnegative integer")
    for name in ("username", "password"):
        if name in settings and not isinstance(settings[name], str):
            raise ValueError(f"redis.{name} must be a string")
    tls = settings.get("tls", {})
    if (not isinstance(tls, dict) or set(tls) - {"ca_file", "server_cert", "server_key"}
            or any(not isinstance(value, str) or not value for value in tls.values())
            or bool(tls.get("server_cert")) != bool(tls.get("server_key"))):
        raise ValueError("Redis TLS requires valid CA/client certificate paths")
    return config


def server_config_argument(parser, argv):
    bootstrap = argparse.ArgumentParser(add_help=False)
    add_env_argument(
        bootstrap, "--config", aliases=("--server-telemetry-config",), dest="server_telemetry_config", env="SERVER_FLEET_TELEMETRY_CONFIG_PATH",
        default=SERVER_TELEMETRY_CONFIG, type=project_path,
    )
    add_env_argument(
        parser, "--config", aliases=("--server-telemetry-config",), dest="server_telemetry_config", env="SERVER_FLEET_TELEMETRY_CONFIG_PATH",
        default=SERVER_TELEMETRY_CONFIG, type=project_path,
    )
    args, _ = bootstrap.parse_known_args(argv)
    try:
        return load_server_telemetry_config(args.server_telemetry_config)
    except (OSError, ValueError) as error:
        parser.error(str(error))

"""Redis Pub/Sub 목적지와 조회 캐시가 공유하는 접속 옵션."""
import math
import os
from urllib.parse import urlsplit

from shared_runtime_config import add_env_argument, project_path


def add_redis_arguments(parser, config):
    settings = config.get("redis", {})
    address = settings.get("addrs", ["127.0.0.1:6380"])[0]
    scheme = "rediss" if settings.get("tls") else "redis"
    default_url = f"{scheme}://{address}/{settings.get('db', 0)}"
    add_env_argument(parser, "--redis-url", dest="server_redis_url",
                     env="SERVER_REDIS_URL", default=default_url)
    add_env_argument(parser, "--redis-namespace", env="SERVER_REDIS_NAMESPACE",
                     default=config.get("namespace", "telemetry_v2"))
    add_env_argument(parser, "--redis-cache-prefix", env="SERVER_REDIS_CACHE_PREFIX",
                     default="telemetry:v2:{query}")
    parser.add_argument("--redis-publish-timeout", type=int,
                        default=settings.get("publish_timeout", 5_000_000_000),
                        help="Tesla redis.publish_timeout: duration in nanoseconds")
    add_env_argument(parser, "--redis-socket-timeout-seconds", env="SERVER_REDIS_SOCKET_TIMEOUT_SECONDS",
                     default=2.0, type=float)


def finish_redis_arguments(parser, args, config):
    parsed = urlsplit(args.server_redis_url)
    try:
        port = parsed.port
    except ValueError:
        parser.error("Invalid Redis URL port")
    if parsed.scheme not in ("redis", "rediss") or not parsed.hostname or port == 0:
        parser.error("SERVER_REDIS_URL requires redis:// or rediss:// with a hostname")
    if not args.redis_namespace or not args.redis_cache_prefix:
        parser.error("Require nonempty Redis namespace and cache prefix")
    if not math.isfinite(args.redis_socket_timeout_seconds) or args.redis_socket_timeout_seconds <= 0:
        parser.error("Require positive finite Redis socket timeout")
    if args.redis_publish_timeout <= 0:
        parser.error("Require positive redis.publish_timeout in nanoseconds")
    settings = config.get("redis", {})
    args.redis_client_options = {
        "decode_responses": True, "protocol": 2,
        "socket_connect_timeout": args.redis_socket_timeout_seconds,
        "socket_timeout": args.redis_socket_timeout_seconds,
        "max_connections": 100, "retry_on_timeout": False,
    }
    if settings.get("username"):
        args.redis_client_options["username"] = settings["username"]
    password = (os.environ.get("REDIS_PASSWORD") or
                os.environ.get("SERVER_REDIS_PASSWORD") or settings.get("password"))
    if password is not None:
        args.redis_client_options["password"] = password
    tls = settings.get("tls", {})
    for field, option in (("ca_file", "ssl_ca_certs"), ("server_cert", "ssl_certfile"),
                          ("server_key", "ssl_keyfile")):
        if field in tls:
            args.redis_client_options[option] = str(project_path(tls[field]))
    if tls and parsed.scheme != "rediss":
        parser.error("Redis TLS settings require a rediss:// URL")
    args.redis_publish_vin_topics = settings.get("publish_vin_topics", True)
    args.redis_subscriber_set_prefix = settings.get("subscriber_set_prefix", "")
    return args

"""조회 Consumer와 조회 API가 공유하는 Redis 캐시 접속 옵션."""
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
    add_env_argument(parser, "--redis-cache-prefix", env="SERVER_REDIS_CACHE_PREFIX",
                     default="telemetry:v2:{query}")
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
    if not args.redis_cache_prefix:
        parser.error("Require a nonempty Redis cache prefix")
    if not math.isfinite(args.redis_socket_timeout_seconds) or args.redis_socket_timeout_seconds <= 0:
        parser.error("Require positive finite Redis socket timeout")
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
    return args

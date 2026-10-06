"""Redis 최신 상태만 조회하는 로컬 API와 차량 정보 화면."""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from redis.exceptions import RedisError

from server_redis_latest_store import RedisLatestVehicleStore
from shared_redis_config import add_redis_arguments, finish_redis_arguments
from shared_runtime_config import add_env_argument, server_config_argument

DASHBOARD_DIRECTORY = Path(__file__).resolve().parents[1] / "frontend"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/vehicle_state.js": ("vehicle_state.js", "text/javascript; charset=utf-8"),
    "/dashboard.js": ("dashboard.js", "text/javascript; charset=utf-8"),
    "/dashboard.css": ("dashboard.css", "text/css; charset=utf-8"),
    "/map_motion.js": ("map_motion.js", "text/javascript; charset=utf-8"),
    "/map_config.json": ("map_config.json", "application/json; charset=utf-8"),
    "/gangnam_roads.geojson": ("gangnam_roads.geojson", "application/geo+json; charset=utf-8"),
    "/vendor/leaflet/leaflet.js": ("vendor/leaflet/leaflet.js", "text/javascript; charset=utf-8"),
    "/vendor/leaflet/leaflet.css": ("vendor/leaflet/leaflet.css", "text/css; charset=utf-8"),
}


def filter_signals(vehicle, selected):
    if selected is None:
        return vehicle
    return {**vehicle,
            "signals": {name: value for name, value in vehicle["signals"].items() if name in selected},
            "observations": {name: value for name, value in vehicle["observations"].items() if name in selected}}


def make_query_handler(store):
    class VehicleQueryHandler(BaseHTTPRequestHandler):
        def _send(self, status, body, content_type="application/json; charset=utf-8"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
            self.send_header("Content-Security-Policy",
                             "default-src 'self'; script-src 'self'; style-src 'self'; "
                             "connect-src 'self'; img-src 'self' data: https://tile.openstreetmap.org; "
                             "object-src 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status, value):
            self._send(status, json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"))

        def do_GET(self):
            parsed = urlsplit(self.path)
            if parsed.path in STATIC_FILES:
                name, content_type = STATIC_FILES[parsed.path]
                self._send(200, (DASHBOARD_DIRECTORY / name).read_bytes(), content_type)
                return
            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                selected = None
                if "signals" in query:
                    selected = set(query["signals"][0].split(","))
                    if not selected or selected - {"location", "speed_mps"}:
                        raise ValueError("signals supports location,speed_mps")
                if parsed.path == "/api/vehicles":
                    limit = int(query.get("limit", ["50"])[0])
                    offset = int(query.get("offset", ["0"])[0])
                    result = store.list_vehicles(limit, offset)
                    result["vehicles"] = [filter_signals(vehicle, selected) for vehicle in result["vehicles"]]
                    self._json(200, result)
                elif parsed.path.startswith("/api/vehicles/"):
                    vehicle_id = unquote(parsed.path[len("/api/vehicles/"):])
                    if not 1 <= len(vehicle_id) <= 512:
                        raise ValueError("Invalid vehicle ID")
                    vehicle = store.get_vehicle(vehicle_id)
                    if vehicle is None:
                        self._json(404, {"error": "Vehicle not found"})
                    else:
                        self._json(200, {"source": "redis", "vehicle": filter_signals(vehicle, selected)})
                elif parsed.path == "/api/health":
                    store.client.ping()
                    self._json(200, {"status": "ready", "source": "redis"})
                else:
                    self._json(404, {"error": "Route not found"})
            except (ValueError, TypeError) as error:
                self._json(400, {"error": str(error)})
            except RedisError:
                self._json(503, {"error": "Redis query is temporarily unavailable"})
            except Exception:
                self._json(500, {"error": "Unable to read vehicle state"})

        def log_message(self, format, *args):
            pass

    return VehicleQueryHandler


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Redis 차량 최신 정보 조회 API 및 화면")
    config = server_config_argument(parser, argv)
    add_env_argument(parser, "--query-api-host", env="SERVER_QUERY_API_HOST", default="127.0.0.1")
    add_env_argument(parser, "--query-api-port", env="SERVER_QUERY_API_PORT", default=8080, type=int)
    add_redis_arguments(parser, config)
    args = parser.parse_args(argv)
    if not 1 <= args.query_api_port <= 65535:
        parser.error("Require a valid query API port")
    return finish_redis_arguments(parser, args, config)


def main():
    args = parse_args()
    store = RedisLatestVehicleStore(
        args.server_redis_url, args.redis_cache_prefix, client_options=args.redis_client_options,
    )
    server = ThreadingHTTPServer(
        (args.query_api_host, args.query_api_port), make_query_handler(store),
    )
    try:
        print(f"[차량 조회 화면] http://{args.query_api_host}:{args.query_api_port}")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()

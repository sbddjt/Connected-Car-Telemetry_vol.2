"""우리 조회 기능: 신호별 최신 상태를 Redis Hash에 원자적으로 반영합니다."""
import json
from datetime import datetime, timezone
from urllib.parse import quote

from redis import Redis
from shared_fleet_telemetry_policy import signals_of, utc_timestamp, validate_event

UPDATE_LATEST = """
local hash_type = redis.call('TYPE', KEYS[1]).ok
local index_type = redis.call('TYPE', KEYS[2]).ok
if hash_type ~= 'none' and hash_type ~= 'hash' then
  return redis.error_reply('Vehicle state key must be a hash')
end
if index_type ~= 'none' and index_type ~= 'zset' then
  return redis.error_reply('Vehicle index key must be a sorted set')
end
local incoming = cjson.decode(ARGV[2])
local updates = {}
local count = 0
for signal, entry in pairs(incoming) do
  local existing = redis.call('HGET', KEYS[1], signal)
  local newer = not existing
  if existing then
    local ok, old = pcall(cjson.decode, existing)
    if not ok or type(old) ~= 'table' or type(old.event_time) ~= 'string'
       or type(old.run_id) ~= 'string' or type(old.sequence_no) ~= 'string'
       or not string.match(old.sequence_no, '^%d+$') then
      return redis.error_reply('Invalid cached observation')
    end
    local a = entry.sequence_no
    local b = old.sequence_no
    local later_sequence = (#a > #b) or (#a == #b and a > b)
    newer = entry.event_time > old.event_time or (
      entry.event_time == old.event_time and entry.run_id == old.run_id and later_sequence)
  end
  if newer then
    updates[signal] = cjson.encode(entry)
    count = count + 1
  end
end
for signal, encoded in pairs(updates) do
  redis.call('HSET', KEYS[1], signal, encoded)
end
redis.call('HSET', KEYS[1], '_vehicle_id', ARGV[1])
redis.call('ZADD', KEYS[2], 'GT', ARGV[3], ARGV[1])
return count
"""


class RedisLatestVehicleStore:
    def __init__(self, url="redis://127.0.0.1:6380/0", prefix="telemetry:v2:{query}",
                 client=None, client_options=None):
        if not prefix:
            raise ValueError("Redis cache prefix must be nonempty")
        self.client = client if client is not None else Redis.from_url(
            url, **(client_options or {"decode_responses": True, "protocol": 2,
                                      "socket_connect_timeout": 2, "socket_timeout": 2}),
        )
        self.prefix = prefix
        self.index_key = f"{prefix}:vehicles"
        self.update_latest = self.client.register_script(UPDATE_LATEST)

    def vehicle_key(self, vehicle_id):
        return f"{self.prefix}:vehicle:{quote(vehicle_id, safe='')}"

    def save(self, event):
        validate_event(event)
        stamp = utc_timestamp(event["event_time"])
        entries = {
            signal: {"value": value, "event_time": stamp, "run_id": event["run_id"],
                     "sequence_no": str(event["sequence_no"]), "event_id": event["event_id"]}
            for signal, value in signals_of(event).items()
        }
        return self.update_latest(
            keys=[self.vehicle_key(event["vehicle_id"]), self.index_key],
            args=[event["vehicle_id"], json.dumps(entries, ensure_ascii=False, allow_nan=False),
                  datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()],
        )

    def save_many(self, events):
        pipeline = self.client.pipeline(transaction=False)
        for event in events:
            validate_event(event)
            stamp = utc_timestamp(event["event_time"])
            entries = {signal:{"value":value, "event_time":stamp, "run_id":event["run_id"],
                       "sequence_no":str(event["sequence_no"]), "event_id":event["event_id"]}
                       for signal,value in signals_of(event).items()}
            self.update_latest(keys=[self.vehicle_key(event["vehicle_id"]),self.index_key],
                               args=[event["vehicle_id"],json.dumps(entries,ensure_ascii=False,allow_nan=False),
                                     datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()],client=pipeline)
        pipeline.execute()

    @staticmethod
    def _decode_state(fields):
        if not fields or "_vehicle_id" not in fields:
            return None
        observations = {}
        for signal in ("location", "speed_mps"):
            if signal in fields:
                value = json.loads(fields[signal])
                observations[signal] = value
        return {
            "vehicle_id": fields["_vehicle_id"],
            "signals": {name: entry["value"] for name, entry in observations.items()},
            "observations": observations,
        }

    def get_vehicle(self, vehicle_id):
        return self._decode_state(self.client.hgetall(self.vehicle_key(vehicle_id)))

    def list_vehicles(self, limit=50, offset=0):
        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("Require 1 <= limit <= 200 and offset >= 0")
        ids = self.client.zrevrange(self.index_key, offset, offset + limit - 1)
        pipeline = self.client.pipeline(transaction=False)
        pipeline.zcard(self.index_key)
        for vehicle_id in ids:
            pipeline.hgetall(self.vehicle_key(vehicle_id))
        results = pipeline.execute()
        vehicles = [self._decode_state(fields) for fields in results[1:]]
        return {"vehicles": [vehicle for vehicle in vehicles if vehicle is not None],
                "total": results[0], "limit": limit, "offset": offset, "source": "redis",
                "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}

    def close(self):
        self.client.close()

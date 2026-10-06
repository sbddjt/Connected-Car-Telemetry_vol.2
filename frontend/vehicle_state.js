"use strict";
// Cache/API and Pub/Sub can arrive out of order: compare every signal independently.
function canonicalObservationTime(stamp) {
  const date = new Date(stamp);
  if (!Number.isFinite(date.getTime())) throw new Error("Invalid observation time");
  const fraction = /\.(\d+)(?:Z|[+-]\d\d:\d\d)$/.exec(stamp)?.[1] ?? "";
  return date.toISOString().slice(0,19) + "." + fraction.padEnd(6,"0").slice(0,6) + "Z";
}
function mergeVehicleState(current, incoming) {
  const result = current ?? {vehicle_id:incoming.vehicle_id,signals:{},observations:{}};
  for (const [signal, next] of Object.entries(incoming.observations)) {
    const old = result.observations[signal];
    const nextTime = canonicalObservationTime(next.event_time);
    const oldTime = old ? canonicalObservationTime(old.event_time) : null;
    if (!old || nextTime > oldTime || (nextTime === oldTime && next.run_id === old.run_id &&
        BigInt(next.sequence_no) > BigInt(old.sequence_no))) {
      result.observations[signal] = next; result.signals[signal] = next.value;
    }
  }
  return result;
}
if (typeof module !== "undefined") module.exports = {mergeVehicleState, canonicalObservationTime};

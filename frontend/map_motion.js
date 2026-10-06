"use strict";
function distanceMeters(from, to) {
  const rad = Math.PI / 180;
  const a = Math.sin((to[0]-from[0])*rad/2)**2 + Math.cos(from[0]*rad)*Math.cos(to[0]*rad)*Math.sin((to[1]-from[1])*rad/2)**2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(Math.max(0,1-a)));
}
function interpolateCoordinate(from, to, progress) {
  const ratio = Math.max(0,Math.min(1,progress));
  return [from[0]+(to[0]-from[0])*ratio, from[1]+(to[1]-from[1])*ratio];
}
function shouldAnimateLocation(previous, next, reducedMotion=false) {
  if (!previous || reducedMotion || previous.run_id !== next.run_id) return false;
  const elapsed = Date.parse(next.event_time)-Date.parse(previous.event_time);
  const from=[previous.value.latitude,previous.value.longitude], to=[next.value.latitude,next.value.longitude];
  return elapsed >= 0 && elapsed <= 5000 && distanceMeters(from,to) <= 150;
}
function latestObservationAge(vehicle, now=Date.now()) {
  const stamps = Object.values(vehicle.observations).map(entry=>Date.parse(entry.event_time)).filter(Number.isFinite);
  return stamps.length ? Math.max(0,(now-Math.max(...stamps))/1000) : Infinity;
}
if (typeof module !== "undefined") module.exports={distanceMeters,interpolateCoordinate,shouldAnimateLocation,latestObservationAge};

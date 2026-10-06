"use strict";
const element = id => document.getElementById(id);
let offset = 0, searchId = "", total = 0, loading = false;
const limit = 50;
const visibleStates = new Map();
let viewGeneration = 0, activeRequest;
function mergeState(incoming) {
  const state = mergeVehicleState(visibleStates.get(incoming.vehicle_id), incoming);
  visibleStates.set(state.vehicle_id, state);
  return state;
}
function changeView(clear=true) {
  viewGeneration++; activeRequest?.abort(); loading = false;
  if (clear) visibleStates.clear();
  render([...visibleStates.values()]); refresh();
}
function timeText(stamp) {
  return stamp ? new Date(stamp).toLocaleTimeString("ko-KR", {hour12:false}) : "—";
}
function cell(value, stamp) {
  const td = document.createElement("td");
  td.append(document.createTextNode(value));
  if (stamp) {
    const span = document.createElement("span");
    span.className = "time"; span.textContent = timeText(stamp) + " 관측";
    td.append(span);
  }
  return td;
}
function render(vehicles) {
  const showLocation = element("show-location").checked, showSpeed = element("show-speed").checked;
  const body = element("vehicles"); body.replaceChildren();
  element("location-heading").hidden = !showLocation;
  element("speed-heading").hidden = !showSpeed;
  for (const vehicle of vehicles) {
    const row = document.createElement("tr");
    row.append(cell(vehicle.vehicle_id));
    if (showLocation) {
      const location = vehicle.signals.location;
      row.append(cell(location ? location.latitude.toFixed(6) + " / " + location.longitude.toFixed(6) : "—",
        vehicle.observations.location?.event_time));
    }
    if (showSpeed) row.append(cell(vehicle.signals.speed_mps == null ? "—" :
      (vehicle.signals.speed_mps * 3.6).toFixed(1), vehicle.observations.speed_mps?.event_time));
    const times = Object.values(vehicle.observations).map(value => Date.parse(value.event_time));
    const latest = times.length ? Math.max(...times) : null;
    const age = latest == null ? null : Math.max(0, Math.floor((Date.now() - latest) / 1000));
    const ageCell = cell(age == null ? "—" : age < 2 ? "방금 관측" : age + "초 전");
    ageCell.className = age != null && age <= 15 ? "recent" : "old";
    row.append(ageCell); body.append(row);
  }
  element("empty").hidden = vehicles.length > 0;
  element("count").textContent = searchId ? searchId + " 조회" : total.toLocaleString() + "대 · " + vehicles.length + "대 표시";
  element("page").textContent = Math.floor(offset / limit) + 1 + "페이지";
  element("previous").disabled = Boolean(searchId) || offset === 0;
  element("next").disabled = Boolean(searchId) || offset + limit >= total;
}
async function refresh() {
  if (loading) return;
  loading = true;
  const generation = viewGeneration;
  activeRequest = new AbortController();
  try {
    const selected = [];
    if (element("show-location").checked) selected.push("location");
    if (element("show-speed").checked) selected.push("speed_mps");
    if (!selected.length) throw new Error("위치 또는 속도를 선택해 주세요.");
    const query = new URLSearchParams({signals:selected.join(","),limit:String(limit),offset:String(offset)});
    const url = searchId ? "/api/vehicles/" + encodeURIComponent(searchId) + "?" + query : "/api/vehicles?" + query;
    const response = await fetch(url, {cache:"no-store",signal:activeRequest.signal});
    const data = await response.json();
    if (generation !== viewGeneration) return;
    if (!response.ok) throw new Error(response.status === 404 ? "해당 차량의 관측 기록이 없습니다." :
      response.status === 503 ? "조회 저장소에 연결할 수 없습니다. 연결 복구 후 다시 갱신합니다." : "차량 정보를 조회하지 못했습니다.");
    total = data.total ?? 1;
    for (const vehicle of data.vehicles ?? [data.vehicle]) mergeState(vehicle);
    const orderedIds = [...new Set([...(data.vehicles ?? [data.vehicle]).map(v => v.vehicle_id), ...visibleStates.keys()])].slice(0,limit);
    for (const id of visibleStates.keys()) if (!orderedIds.includes(id)) visibleStates.delete(id);
    render(orderedIds.map(id => visibleStates.get(id)));
    element("status").className = "";
    element("status").textContent = new Date().toLocaleTimeString("ko-KR", {hour12:false}) + " 갱신";
  } catch (error) {
    if (generation !== viewGeneration || error.name === "AbortError") return;
    element("status").className = "error"; element("status").textContent = error.message;
  } finally { if (generation === viewGeneration) loading = false; }
}
element("search-form").addEventListener("submit", event => {
  event.preventDefault(); searchId = element("vehicle-id").value.trim(); offset = 0; changeView();
});
element("show-all").addEventListener("click", () => { searchId = ""; element("vehicle-id").value = ""; offset = 0; changeView(); });
for (const id of ["show-location","show-speed"]) element(id).addEventListener("change", () => changeView(false));
element("previous").addEventListener("click", () => { offset = Math.max(0,offset-limit); changeView(); });
element("next").addEventListener("click", () => { offset += limit; changeView(); });
setInterval(() => { if (element("auto-refresh").checked && !document.hidden) refresh(); },1000);
refresh();
window.addEventListener("beforeunload", () => activeRequest?.abort());

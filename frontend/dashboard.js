"use strict";
const element = id => document.getElementById(id);
const states = new Map(), markers = new Map(), motions = new Map(), trails = new Map();
const carSvg = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m5 9 2-5h10l2 5m-15 0h16v9H4Zm2 9v2m12-2v2M7 13h2m6 0h2"/></svg>';
const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
let map, roadLayer, tiles, trailLayer, selectedId = null, scope = "all", query = "";
let loading=false, total=0, apiError=false, animationFrame=0, initialFit=false, lastSynced=null;
const refreshInterval=1000, recentSeconds=15, maximumVehicles=1000;
function text(id, value) { element(id).textContent=value; }
function timeText(stamp) { return stamp ? new Date(stamp).toLocaleTimeString("ko-KR",{hour12:false}) : "—"; }
function speedOf(vehicle) { const speed=vehicle.signals.speed_mps; return Number.isFinite(speed) ? speed*3.6 : null; }
function coordinateOf(vehicle) {
  const location=vehicle.signals.location;
  return location && Number.isFinite(location.latitude) && Number.isFinite(location.longitude) ? [location.latitude,location.longitude] : null;
}
function ageText(age) { return !Number.isFinite(age) ? "관측 대기" : age < 2 ? "방금 관측" : age < 60 ? Math.floor(age)+"초 전 관측" : age < 3600 ? Math.floor(age/60)+"분 전 관측" : Math.floor(age/3600)+"시간 전 관측"; }
function visibleVehicles() {
  return [...states.values()].filter(vehicle=>(!query || vehicle.vehicle_id.toLowerCase().includes(query.toLowerCase())) && (scope!=="recent" || latestObservationAge(vehicle)<=recentSeconds))
    .sort((a,b)=>(latestObservationAge(a)<=recentSeconds?0:1)-(latestObservationAge(b)<=recentSeconds?0:1) || a.vehicle_id.localeCompare(b.vehicle_id,undefined,{numeric:true}));
}
function setConnection() {
  const paused=!element("auto-refresh").checked;
  element("connection").className="sync-badge"+(apiError?" error":paused?" paused":"");
  text("connection-label",apiError?"조회 연결 대기":paused?"자동 갱신 정지":"최신 상태 동기화");
}
function selectVehicle(id, pan=true) {
  if (!states.has(id)) return;
  selectedId=id;
  const location=coordinateOf(states.get(id));
  if (map && location && pan) map.panInside(location,{paddingTopLeft:[50,120],paddingBottomRight:window.innerWidth>900?[300,90]:[40,310],animate:!reducedMotion.matches});
  render();
}
function startAnimation() {
  if (animationFrame || !motions.size) return;
  animationFrame=requestAnimationFrame(animate);
}
function animate(now) {
  animationFrame=0;
  for (const [id,motion] of motions) {
    const marker=markers.get(id);
    if (!marker) { motions.delete(id); continue; }
    const progress=(now-motion.started)/800;
    marker.setLatLng(interpolateCoordinate(motion.from,motion.to,reducedMotion.matches?1:progress));
    if (progress>=1 || reducedMotion.matches) motions.delete(id);
  }
  startAnimation();
}
function applyVehicle(incoming) {
  const previous=states.get(incoming.vehicle_id)?.observations.location;
  const vehicle=mergeVehicleState(states.get(incoming.vehicle_id),incoming);
  states.set(vehicle.vehicle_id,vehicle);
  const next=vehicle.observations.location, location=coordinateOf(vehicle);
  if (!map || !location) return;
  let marker=markers.get(vehicle.vehicle_id);
  if (!marker) {
    marker=L.marker(location,{icon:L.divIcon({className:"vehicle-pin",html:'<span class="car-pin">'+carSvg+'</span>',iconSize:[27,27],iconAnchor:[13.5,13.5]}),keyboard:true});
    marker.on("click",()=>selectVehicle(vehicle.vehicle_id,false));
    const label=document.createElement("span"); marker.bindTooltip(label,{direction:"top",offset:[0,-18]}); marker._fleetLabel=label;
    markers.set(vehicle.vehicle_id,marker);
  }
  if (!previous || next !== previous) {
    const continuous=shouldAnimateLocation(previous,next,reducedMotion.matches);
    if (previous && continuous) {
      const current=marker.getLatLng(); motions.set(vehicle.vehicle_id,{from:[current.lat,current.lng],to:location,started:performance.now()});
    } else { motions.delete(vehicle.vehicle_id); marker.setLatLng(location); }
    let trail=trails.get(vehicle.vehicle_id)??[];
    if (previous && !shouldAnimateLocation(previous,next)) trail=[];
    trail.push(location); trails.set(vehicle.vehicle_id,trail.slice(-80));
    if (selectedId===vehicle.vehicle_id && element("follow-vehicle").checked) map.panTo(location,{animate:false});
    startAnimation();
  }
}
function renderMetrics() {
  const vehicles=[...states.values()]; const recent=vehicles.filter(vehicle=>latestObservationAge(vehicle)<=recentSeconds);
  const speeds=vehicles.filter(vehicle=>vehicle.observations.speed_mps && (Date.now()-Date.parse(vehicle.observations.speed_mps.event_time))/1000<=recentSeconds).map(speedOf).filter(Number.isFinite);
  text("metric-total",total.toLocaleString()); text("metric-recent",recent.length.toLocaleString());
  text("metric-moving",speeds.filter(speed=>speed>=1).length.toLocaleString());
  text("metric-speed",speeds.length?(speeds.reduce((a,b)=>a+b,0)/speeds.length).toFixed(1):"—");
  text("loaded-count",states.size<total?states.size+"대 표시 / 전체 "+total+"대":"차량별 마지막 관측 기준");
}
function renderList(vehicles) {
  const container=element("vehicle-list"), scroll=container.scrollTop, fragment=document.createDocumentFragment();
  for (const vehicle of vehicles) {
    const age=latestObservationAge(vehicle), speed=speedOf(vehicle);
    const row=document.createElement("button"); row.type="button"; row.dataset.vehicleId=vehicle.vehicle_id;
    row.className="vehicle-row"+(vehicle.vehicle_id===selectedId?" selected":""); row.setAttribute("aria-pressed",String(vehicle.vehicle_id===selectedId));
    const icon=document.createElement("span"); icon.className="vehicle-row-icon"; icon.innerHTML=carSvg;
    const copy=document.createElement("span"); copy.className="vehicle-row-copy";
    const id=document.createElement("span"); id.className="vehicle-row-id"; id.textContent=vehicle.vehicle_id;
    const status=document.createElement("span"); status.className="vehicle-row-status"+(age>recentSeconds?" stale":"");
    const dot=document.createElement("i"); dot.className="status-dot";
    status.append(dot,document.createTextNode(age<=recentSeconds?"최근 관측 · "+ageText(age):"마지막 위치 · "+ageText(age))); copy.append(id,status);
    const velocity=document.createElement("span"); velocity.className="vehicle-row-speed"; velocity.textContent=speed==null?"—":speed.toFixed(0);
    const unit=document.createElement("small"); unit.textContent="km/h"; velocity.append(unit); row.append(icon,copy,velocity); fragment.append(row);
  }
  container.replaceChildren(fragment); container.scrollTop=scroll; text("list-count",vehicles.length); element("empty").hidden=vehicles.length>0;
}
function renderDetail() {
  const vehicle=states.get(selectedId); if (!vehicle) return;
  const location=coordinateOf(vehicle), speed=speedOf(vehicle), age=latestObservationAge(vehicle);
  text("selected-id",vehicle.vehicle_id); text("selected-status",age<=recentSeconds?"최근 관측 신호":"마지막 관측 상태"); text("selected-speed",speed==null?"—":speed.toFixed(1));
  element("speed-meter").style.width=Math.min(100,speed??0)+"%";
  text("selected-latitude",location?location[0].toFixed(6):"—"); text("selected-longitude",location?location[1].toFixed(6):"—");
  text("location-time",timeText(vehicle.observations.location?.event_time)); text("speed-time",vehicle.observations.speed_mps?timeText(vehicle.observations.speed_mps.event_time)+" 속도 관측":"속도 관측 대기"); text("selected-age",ageText(age));
}
function renderMap(vehicles) {
  if (!map) return;
  const visible=new Set(vehicles.map(vehicle=>vehicle.vehicle_id));
  for (const [id,marker] of markers) {
    if (!visible.has(id)) { if (map.hasLayer(marker)) map.removeLayer(marker); continue; }
    if (!map.hasLayer(marker)) marker.addTo(map);
    const vehicle=states.get(id), age=latestObservationAge(vehicle), selected=id===selectedId;
    marker.getElement()?.classList.toggle("selected",selected); marker.getElement()?.classList.toggle("stale",age>recentSeconds);
    marker.getElement()?.setAttribute("aria-label","차량 "+id); marker.setZIndexOffset(selected?1000:0);
    marker._fleetLabel.textContent=id+" · "+(speedOf(vehicle)==null?"—":speedOf(vehicle).toFixed(1)+" km/h");
  }
  trailLayer.clearLayers();
  if (element("show-trail").checked && visible.has(selectedId)) { const points=trails.get(selectedId)??[]; if (points.length>1) L.polyline(points,{color:"#13a881",weight:3,opacity:.6,lineCap:"round"}).addTo(trailLayer); }
  text("map-subtitle","지도에 "+vehicles.filter(coordinateOf).length+"대 표시 · 1초 주기 조회");
}
function render() { const vehicles=visibleVehicles(); renderMetrics(); renderList(vehicles); renderDetail(); renderMap(vehicles); setConnection(); }
function fitVehicles() {
  if (!map) return;
  const coordinates=visibleVehicles().map(coordinateOf).filter(Boolean);
  if (coordinates.length) map.fitBounds(coordinates,{paddingTopLeft:[60,140],paddingBottomRight:window.innerWidth>900?[310,80]:[30,310],maxZoom:17,animate:!reducedMotion.matches});
}
async function refresh() {
  if (loading) return; loading=true;
  try {
    const response=await fetch("/api/vehicles?limit=200",{cache:"no-store"});
    if (!response.ok) throw new Error(response.status===503?"조회 저장소 연결 대기 · 복구 후 다시 갱신":"차량 정보를 조회하지 못했습니다");
    const data=await response.json(); total=data.total; const records=[...data.vehicles];
    const offsets=[]; for(let offset=200;offset<Math.min(total,maximumVehicles);offset+=200) offsets.push(offset);
    const pages=await Promise.all(offsets.map(async offset=>{ const page=await fetch("/api/vehicles?limit=200&offset="+offset,{cache:"no-store"}); if (!page.ok) throw new Error("추가 차량 목록을 조회하지 못했습니다"); return (await page.json()).vehicles; }));
    for(const page of pages) records.push(...page); for(const vehicle of records) applyVehicle(vehicle);
    if (states.size > maximumVehicles) {
      const keep = new Set([...states.values()].sort((a,b)=>latestObservationAge(a)-latestObservationAge(b)).slice(0,maximumVehicles).map(vehicle=>vehicle.vehicle_id));
      if(selectedId && !keep.has(selectedId)) { keep.delete([...keep].pop()); keep.add(selectedId); }
      for(const id of states.keys()) if(!keep.has(id)) {
        const marker=markers.get(id); if(map && marker) map.removeLayer(marker);
        states.delete(id); markers.delete(id); motions.delete(id); trails.delete(id);
      }
    }
    if (!selectedId && records.length) selectedId=visibleVehicles()[0]?.vehicle_id??records[0].vehicle_id;
    apiError=false; lastSynced=Date.now(); element("status").className=""; text("status",timeText(lastSynced)+" 갱신"); render();
    if (map && !initialFit && records.some(coordinateOf)) { initialFit=true; fitVehicles(); }
  } catch(error) { apiError=true; element("status").className="error"; text("status",error.message); setConnection(); }
  finally { loading=false; }
}
async function initializeMap() {
  try {
    const response=await fetch("/map_config.json"); if(!response.ok) throw new Error("지도 설정을 읽지 못했습니다"); const config=await response.json();
    map=L.map("map",{zoomControl:true,preferCanvas:true}).setView(config.center,config.zoom);
    map.attributionControl.setPrefix(false); map.attributionControl.addAttribution('&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap contributors</a>');
    trailLayer=L.layerGroup().addTo(map); let failedTiles=0, loadedTiles=0;
    tiles=L.tileLayer(config.tile_url,{maxZoom:19,keepBuffer:1,updateWhenIdle:true});
    tiles.on("tileerror",()=>{failedTiles++; if(failedTiles>=3 && loadedTiles===0) element("map-notice").hidden=false;});
    tiles.on("tileload",()=>{loadedTiles++; element("map-notice").hidden=true; if(roadLayer) roadLayer.setStyle({opacity:.18,weight:1.5});}); tiles.addTo(map);
    fetch("/gangnam_roads.geojson").then(response=>{if(!response.ok) throw new Error("도로망을 읽지 못했습니다"); return response.json();}).then(roads=>{ roadLayer=L.geoJSON(roads,{style:{color:"#90a8a4",weight:loadedTiles?1.5:5,opacity:loadedTiles ? .18 : .6},interactive:false}).addTo(map); roadLayer.bringToBack(); }).catch(()=>{ if(!loadedTiles) { element("map-notice").hidden=false; text("map-notice","지도를 불러오지 못했습니다. 차량 정보는 목록에서 확인할 수 있습니다."); } });
    element("center-gangnam").addEventListener("click",()=>{element("follow-vehicle").checked=false; map.setView(config.center,config.zoom,{animate:!reducedMotion.matches});});
    map.on("dragstart",()=>{element("follow-vehicle").checked=false;}); new ResizeObserver(()=>map.invalidateSize()).observe(element("map"));
    for(const vehicle of states.values()) applyVehicle(vehicle); render(); if(states.size && !initialFit) { initialFit=true; fitVehicles(); }
  } catch(error) { element("map-notice").hidden=false; text("map-notice",error.message); }
}
element("vehicle-list").addEventListener("click",event=>{const row=event.target.closest("[data-vehicle-id]"); if(row) selectVehicle(row.dataset.vehicleId);});
element("vehicle-id").addEventListener("input",event=>{query=event.target.value.trim(); render();});
element("search-form").addEventListener("submit",async event=>{
  event.preventDefault(); const id=element("vehicle-id").value.trim(); if (!id) { query=""; render(); return; }
  const local=visibleVehicles().find(vehicle=>vehicle.vehicle_id===id)??visibleVehicles()[0]; if(local) { selectVehicle(local.vehicle_id); return; }
  try { const response=await fetch("/api/vehicles/"+encodeURIComponent(id),{cache:"no-store"}); if(!response.ok) throw new Error(response.status===404?"해당 차량의 관측 기록이 없습니다":"차량 조회 연결 대기"); applyVehicle((await response.json()).vehicle); query=id; scope="all"; for(const tab of document.querySelectorAll("[data-scope]")) tab.classList.toggle("active",tab.dataset.scope===scope); selectVehicle(id); }
  catch(error) { text("status",error.message); element("status").className="error"; }
});
for(const tab of document.querySelectorAll("[data-scope]")) tab.addEventListener("click",()=>{scope=tab.dataset.scope; for(const other of document.querySelectorAll("[data-scope]")) other.classList.toggle("active",other===tab); render();});
element("fit-vehicles").addEventListener("click",()=>{element("follow-vehicle").checked=false; fitVehicles();});
element("follow-vehicle").addEventListener("change",()=>{if(selectedId) selectVehicle(selectedId);});
element("show-trail").addEventListener("change",()=>renderMap(visibleVehicles()));
element("auto-refresh").addEventListener("change",()=>{setConnection(); if(element("auto-refresh").checked) refresh();}); element("refresh").addEventListener("click",refresh);
function tick() { text("clock",new Date().toLocaleTimeString("ko-KR",{hour12:false})); render(); if(element("auto-refresh").checked && !document.hidden) refresh(); }
setInterval(tick,refreshInterval); initializeMap(); refresh();

let loadingPipeline=false;
async function refreshPipeline() {
  if(loadingPipeline) return;
  loadingPipeline=true;
  try {
    const response=await fetch("/api/pipeline",{cache:"no-store",signal:AbortSignal.timeout(4000)});
    if(!response.ok) throw new Error("Monitoring unavailable");
    const {components}=await response.json();
    const fresh=name=>components[name] && !components[name].stale ? components[name] : null;
    const collector=fresh("vehicle-collector"), sender=fresh("vehicle-sender"), history=fresh("history-consumer"), redis=fresh("redis-consumer");
    const set=(id,record,value)=>{text(id,record?value:"—");element(id).parentElement.classList.toggle("unavailable",!record);};
    set("pipeline-active",collector,collector?.active_vehicles+" 대");
    set("pipeline-collected",collector,collector?.events_per_second+" 건/s");
    set("pipeline-acked",sender,sender?.events_per_second+" 건/s");
    set("pipeline-pending",sender,sender?.pending_events?.toLocaleString()+" 건");
    text("pipeline-dropped",sender?"용량 초과 삭제 "+sender.capacity_dropped+"건":"확인 대기 기록");
    for(const [name,record] of [["history",history],["redis",redis]]) {
      set("pipeline-"+name,record,record?.events_per_second+" 건/s");
      text("pipeline-"+name+"-delay",record?.retrying?"저장 재시도 중":Number.isFinite(record?.processing_delay_seconds)?"관측 → 처리 "+record.processing_delay_seconds.toFixed(1)+"초":"처리 지연 · 5초 표본");
    }
  } catch(error) {
    for(const id of ["active","collected","acked","pending","history","redis"]) text("pipeline-"+id,"—");
  } finally {loadingPipeline=false;}
}
setInterval(()=>{if(!document.hidden) refreshPipeline();},5000);
refreshPipeline();

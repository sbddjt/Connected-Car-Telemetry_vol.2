"use strict";
const assert = require("node:assert/strict");
const {mergeVehicleState,canonicalObservationTime} = require("../frontend/vehicle_state.js");
const state = (time,seq,value,signal="location",run="run") => ({vehicle_id:"car",observations:{
  [signal]:{event_time:time,sequence_no:String(seq),run_id:run,value}}});
let latest=mergeVehicleState(null,state("2026-10-06T00:00:02.000001Z",1,"new"));
latest=mergeVehicleState(latest,state("2026-10-06T00:00:02.000000Z",99,"old"));
assert.equal(latest.signals.location,"new");
latest=mergeVehicleState(latest,state("2026-10-06T00:00:01.000Z",2,10,"speed_mps"));
assert.equal(latest.signals.location,"new"); assert.equal(latest.signals.speed_mps,10);
latest=mergeVehicleState(latest,state("2026-10-06T09:00:02.000001+09:00","1152921504606846976","tie"));
latest=mergeVehicleState(latest,state("2026-10-06T00:00:02.000001Z","1152921504606846977","next"));
assert.equal(latest.signals.location,"next");
latest=mergeVehicleState(latest,state("2026-10-06T00:00:02.000001Z","1152921504606846978","other-run","location","other"));
assert.equal(latest.signals.location,"next");
assert.equal(canonicalObservationTime("2026-10-06T00:00:01Z"),"2026-10-06T00:00:01.000000Z");
console.log("Vehicle state ordering: passed");

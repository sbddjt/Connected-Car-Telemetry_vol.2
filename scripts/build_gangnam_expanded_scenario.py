"""강남역·역삼·선릉 OSM 도로망과 재현 가능한 30분 차량 유입 시나리오 생성."""
import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download",action="store_true",help="캐시 대신 OSM 도로 데이터를 한 번 내려받습니다")
    args=parser.parse_args()
    profile=json.loads((ROOT/"config/gangnam_expanded_scenario.json").read_text(encoding="utf-8"))
    directory=ROOT/"scenario/gangnam_expanded"
    directory.mkdir(parents=True,exist_ok=True)
    source=directory/"gangnam_expanded.osm.xml.gz"
    west,south,east,north=profile["bbox"]
    if args.download or not source.exists():
        query=f'[out:xml][timeout:40];(way["highway"]({south},{west},{north},{east});>;);out body;'
        url=profile["osm_endpoint"]+"?"+urllib.parse.urlencode({"data":query})
        request=urllib.request.Request(url,headers={"User-Agent":"Connected-Car-Telemetry-vol2 local SUMO scenario builder"})
        with urllib.request.urlopen(request,timeout=50) as response:
            body=response.read()
        if b"<osm" not in body[:1000]:
            raise ValueError("Source did not return OSM XML")
        source.write_bytes(gzip.compress(body,mtime=0))
    netconvert=shutil.which("netconvert")
    if not netconvert:
        raise RuntimeError("Install SUMO and put netconvert on PATH")
    sumo_root=Path(os.environ.get("SUMO_HOME",str(Path(netconvert).resolve().parents[1])))
    network=directory/"gangnam_expanded.net.xml.gz"
    subprocess.run([netconvert,"--osm-files",str(source),"--output-file",str(network),
                    "--output.street-names","true","--geometry.remove","true","--junctions.join","true",
                    "--tls.guess-signals","true","--tls.discard-simple","true","--tls.join","true",
                    "--tls.default-type","actuated","--ramps.guess","true","--keep-edges.by-vclass","passenger",
                    "--keep-edges.in-geo-boundary",",".join(map(str,profile["bbox"])),"--no-warnings","true"],check=True)
    temporary=ROOT/"data/runtime/scenario-build"
    temporary.mkdir(parents=True,exist_ok=True)
    routes=temporary/"gangnam_expanded.rou.xml"
    subprocess.run([sys.executable,str(sumo_root/"tools/randomTrips.py"),"-n",str(network),
                    "-o",str(temporary/"gangnam_expanded.trips.xml"),"-r",str(routes),
                    "--begin","0","--end",str(profile["duration_seconds"]),"--period",str(profile["departure_period_seconds"]),
                    "--seed",str(profile["seed"]),"--min-distance","800","--fringe-factor","3",
                    "--vehicle-class","passenger","--prefix","gangnam-",
                    "--trip-attributes",'departLane="best" departSpeed="max"',"--length","--lanes","--remove-loops","--threads","2"],check=True)
    (directory/"gangnam_expanded.rou.xml.gz").write_bytes(gzip.compress(routes.read_bytes(),mtime=0))
    subprocess.run([sys.executable,str(ROOT/"scripts/build_gangnam_roads.py"),"--network",str(network)],check=True)
    print("Expanded scenario and local map roads rebuilt. Active limit is in config/gangnam_expanded_sumo.sumocfg.")


if __name__=="__main__":main()

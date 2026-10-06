"""기존 SUMO 강남 도로망을 지도용 GeoJSON으로 내보냅니다. 타일 다운로드는 하지 않습니다."""
import argparse
import gzip
import json
from pathlib import Path
from xml.etree import ElementTree as ET
import traci

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", type=Path, default=ROOT / "scenario/gangnam/osm.net.xml.gz")
    parser.add_argument("--output", type=Path, default=ROOT / "frontend/gangnam_roads.geojson")
    args = parser.parse_args()
    network_path = args.network.resolve()
    with gzip.open(network_path, "rb") as source:
        network = ET.parse(source).getroot()
    traci.start(["sumo", "--net-file", str(network_path), "--no-step-log", "true",
                 "--duration-log.disable", "true"], label="map-export")
    connection = traci.getConnection("map-export")
    cache, features = {}, []
    try:
        for edge in network.findall("edge"):
            if edge.get("function") == "internal" or edge.get("id", "").startswith(":"):
                continue
            lane = edge.find("lane")
            if lane is None or not lane.get("shape"):
                continue
            coordinates = []
            for point in lane.get("shape").split():
                x, y = map(float, point.split(",")[:2])
                if (x, y) not in cache:
                    cache[x, y] = [round(value, 7) for value in connection.simulation.convertGeo(x, y)]
                coordinates.append(cache[x, y])
            if len(coordinates) >= 2:
                features.append({"type": "Feature", "properties": {"name": edge.get("name", "")},
                                 "geometry": {"type": "LineString", "coordinates": coordinates}})
    finally:
        connection.close()
    payload = {"type": "FeatureCollection", "features": features}
    target = args.output.resolve()
    target.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Exported {len(features)} roads to {target}")


if __name__ == "__main__":
    main()

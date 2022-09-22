import time

import osmium
from shapely import wkt
import geopandas
from shapely.geometry import LineString
from typing import List

from src.utils.merging_utils import get_merged

# %%
wkt_factory = osmium.geom.WKTFactory()


class HighwayHandler(osmium.SimpleHandler):
    highways_type = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    highways_with_level = dict(zip(highways_type, [1, 2, 3, 4, 5]))

    def __init__(self):
        super().__init__()
        self.highways = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}

    def way(self, w):
        self.get_ways(w)

    def get_ways(self, w):
        way_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        highway = w.tags.get("highway")
        if highway in HighwayHandler.highways_type:
            line = wkt.loads(wkt_factory.create_linestring(w))
            self.append_way_attribute(way_id, name, line, self.get_way_level(highway))

    def get_way_level(self, highway_tag: str) -> int:
        return HighwayHandler.highways_with_level.get(highway_tag)

    def append_way_attribute(self, way_id: str, name: str, line: LineString, level: int):
        self.highways.get('POLYGON_ID').append(way_id)
        self.highways.get('POLYGON_NAME').append(name)
        self.highways.get('POLYGON_STR').append(line)
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        self.highways.get('HOFN_TYPE').append(7)
        self.highways.get('HOFN_LEVEL').append(level)


h = HighwayHandler()
h.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", locations=True, idx="flex_mem")
result = geopandas.GeoDataFrame(h.highways, geometry="POLYGON_STR")
result.to_file("data\\output\\highway\\unmerged_highway.geojson", driver="GeoJSON")

# %%

unmerged_highways = geopandas.read_file("data\\output\\highway\\unmerged_highway.geojson")
merged_start_time = time.time()
merged_highway_dict = get_merged(unmerged_highways)
merged_end_time = time.time()
print(f"Merged highway process completed, taking {merged_end_time - merged_start_time}")
result = geopandas.GeoDataFrame.from_dict(merged_highway_dict, orient="index")
result.to_file("data\\output\\highway\\merged_highway.geojson", driver="GeoJSON")


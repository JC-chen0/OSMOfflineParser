import time

import geopandas
from shapely import wkt
from shapely.geometry import LineString
import osmium

from src.utils.merging_utils import get_relation_polygon, prepare_data

# %%
wkt_factory = osmium.geom.WKTFactory()


class HighwayHandler(osmium.SimpleHandler):
    highways_type = ["motorway", "trunk", "primary", "secondary", "tertiary", "unclassified"]
    highways_with_level = dict(zip(highways_type, [1, 2, 3, 4, 5, 6]))

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
            if self.get_way_level(highway) == 6:
                line = wkt.loads(wkt_factory.create_linestring(w))
                self.append_way_attribute(way_id, name, line, self.get_way_level(highway))

    def get_way_level(self, highway_tag: str) -> int:
        return HighwayHandler.highways_with_level.get(highway_tag)

    def append_way_attribute(self, way_id: str, name: str, line: LineString, level: int):
        self.highways.get('POLYGON_ID').append(way_id)
        self.highways.get('POLYGON_NAME').append(name)
        self.highways.get('POLYGON_STR').append(line)
        self.highways.get('HOFN_TYPE').append(7)
        self.highways.get('HOFN_LEVEL').append(level)


h = HighwayHandler()
h.apply_file("data\\input\\country\\malaysia-singapore-brunei-latest.osm.pbf", locations=True, idx="flex_mem")
result = geopandas.GeoDataFrame(h.highways, geometry="POLYGON_STR")
result.to_file("data\\output\\highway\\unmerged_malaysia_highway_6.geojson", driver="GeoJSON")

# %%
malaysia_polygon_wkt = get_relation_polygon("2108121").wkt
# %%
start_time = time.time()
malaysia_highway_6 = prepare_data("data\\output\\highway\\unmerged_malaysia_highway_6.geojson",                    malaysia_polygon_wkt)


print(f"Preparing data process completed, taking {time.time() - start_time}")
# %%
# merged_malaysia_highway_6 = get_merged(malaysia_highway_6)

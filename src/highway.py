import time
from copy import deepcopy
from typing import Dict

import osmium
from geopandas import GeoDataFrame
from shapely import wkt
import geopandas
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import linemerge

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
        self.highways.get('HOFN_TYPE').append(7)
        self.highways.get('HOFN_LEVEL').append(level)


h = HighwayHandler()
h.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", locations=True, idx="flex_mem")
result = geopandas.GeoDataFrame(h.highways, geometry="POLYGON_STR")
result.to_file("data\\output\\highway\\unmerged_highway.geojson", driver="GeoJSON")


# %%

def reverse_linestring_coords(geometry) -> LineString:
    geometry.coords = list(geometry.coords)[::-1]

def is_continuous(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_tail or tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return (head == compare_head and tail != compare_tail) or (tail == compare_tail and head != compare_head)


def get_merged_highway(unmerged_highways_df: GeoDataFrame) -> Dict:
    highway_dict = unmerged_highways_df.set_index(unmerged_highways_df["POLYGON_ID"]).to_dict('index')
    highway_merge_dict = deepcopy(highway_dict)
    start_time = time.time()
    processed_list = []
    for highway_id, highway_dict in highway_dict.items():
        if highway_id in processed_list:
            continue
        geometry = highway_dict["geometry"]
        level = highway_dict["HOFN_LEVEL"]
        merging = True
        while merging:
            for compare_id, compare_highway in highway_merge_dict.items():
                compare_geometry = compare_highway["geometry"]
                compare_level = compare_highway["HOFN_LEVEL"]

                if compare_id == list(highway_merge_dict.keys())[-1]:
                    print(f"{highway_id} merge process completed, start another round.")
                    merging = False
                    break

                if level != compare_level or highway_id == compare_id:
                    continue

                ############################################
                if is_reverse_needed(geometry, compare_geometry):
                    compare_geometry = reverse_linestring_coords(compare_geometry)
                elif is_continuous(geometry, compare_geometry):
                    merge_linestring = linemerge([compare_geometry, geometry])
                    geometry = merge_linestring
                    highway_merge_dict.get(highway_id)["geometry"] = geometry

                    # remove merged id
                    highway_merge_dict.pop(compare_id)
                    processed_list.append(compare_id)
                    print(f"{highway_id} merge with {compare_id}, break, {compare_id} will be removed.")
                    break  # break inner for loop to start another round of merging
    return highway_merge_dict


# %%
unmerged_highways = geopandas.read_file("data\\output\\highway\\unmerged_highway.geojson")
# %%
merged_start_time = time.time()
merged_highway_dict = get_merged_highway(unmerged_highways)
merged_end_time = time.time()
print(f"Merged highway process completed, taking {merged_end_time - merged_start_time}")


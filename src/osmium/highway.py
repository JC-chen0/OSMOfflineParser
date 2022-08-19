import osmium
from shapely import wkt
import geopandas
from shapely.geometry import Point, LineString, Polygon

wktfab = osmium.geom.WKTFactory()


class HighwayHandler(osmium.SimpleHandler):
    highways_type = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    highways_with_level = dict(zip(highways_type, [1, 2, 3, 4, 5]))

    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.road_names = []
        self.highways = {'id': [], 'way_name': [], 'way_geometry': [], 'highway': [], 'way_level': []}

    def way(self, w):
        self.get_ways(w)

    def get_ways(self, w):
        way_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        highway = w.tags.get("highway")
        if highway in HighwayHandler.highways_type:
            line = wkt.loads(wktfab.create_linestring(w))
            self.append_way_attribute(way_id, name, line, highway, self.get_way_level(highway))

    def get_way_level(self, highway_tag: str) -> int:
        return HighwayHandler.highways_with_level.get(highway_tag)

    def append_way_attribute(self, way_id: str, name: str, line: LineString, highway: str, level: int):
        self.highways.get('id').append(way_id)
        self.highways.get('way_name').append(name)
        self.highways.get('way_geometry').append(line)
        self.highways.get('highway').append(highway)
        self.highways.get('way_level').append(level)

    def box_in_taipei(self):
        taipei = osmium.osm.Box


h = HighwayHandler()
h.apply_file("data//input//taiwan-latest.osm.pbf", locations=True, idx="flex_mem")
result = geopandas.GeoDataFrame(h.highways, geometry="way_geometry")

# %%

# TODO: Link seperated ways
# step 0: search for related name
# step 1: find wkt head and tail . If self head or tail match other's head or tail, then link them

ways = result.loc[result.way_name == "重慶北路一段"]
ways = ways[ways.way_geometry.is_valid]

# %%
ways_dict = dict()
processed_id_way = []
for idx, row in ways.iterrows():
    geometry = row.way_geometry

    if row.id in processed_id_way:
        continue

    # init
    if row.id not in ways_dict:
        ways_dict[row.id] = []

    # get head and tail in split() way
    # head = row.coords[0]
    # tail = row.coords[-1]

    # boundary
    head, tail = geometry.boundary

    for sub_geometry in ways.way_geometry.tolist():
        sub_head, sub_tail = sub_geometry.boundary

        # Same linestring
        if head == sub_head and tail == sub_tail:
            pass
        if head == sub_head:
            pass
        if tail == sub_head:
            pass
        if head == sub_tail:
            pass
        if tail == sub_tail:
            pass

        processed_id_way.append(row.id)
        ways_dict[row.id].append(row)

# tsv
# result.to_csv(f"ways_level_1.tsv", sep="\t")
# geojson
# result.to_file("..//..//data//output//ways_level.geojson", driver="GeoJSON")

# %%
# Second method: linemarge?
from shapely.geometry import MultiLineString, LineString
from functools import reduce
from shapely.ops import linemerge

ways_union = reduce(LineString.union, ways.way_geometry)
ways_merge = linemerge(ways_union)
# 　No polygon id -> depreciated

import osmium
from shapely import wkt
import geopandas
from shapely.geometry import Point, LineString, Polygon

wktfab = osmium.geom.WKTFactory()


class HighwayHandler(osmium.SimpleHandler):
    highways = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    highways_with_level = dict(zip(highways, [1, 2, 3, 4, 5]))

    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.road_names = []
        self.highways = {'id': [],'way_name': [], 'way_geometry': [], 'highway': [], 'way_level': []}

    def get_ways(self, w):
        way_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        highway = w.tags.get("highway")
        if highway in HighwayHandler.highways:
            line = wkt.loads(wktfab.create_linestring(w))
            self.append_way_attribute(way_id, name, line, highway, self.get_way_level(highway))

    def way(self, w):
        self.get_ways(w)

    def get_way_level(self, highway_tag: str) -> int:
        return HighwayHandler.highways_with_level.get(highway_tag)

    def append_way_attribute(self, way_id: str, name: str, line: LineString, highway: str, level: int):
        self.highways.get('id').append(way_id)
        self.highways.get('way_name').append(name)
        self.highways.get('way_geometry').append(line)
        self.highways.get('highway').append(highway)
        self.highways.get('way_level').append(level)


if __name__ == '__main__':
    h = HighwayHandler()
    h.apply_file("taiwan-latest.osm.pbf", locations=True, idx="flex_mem")
    result = geopandas.GeoDataFrame(h.highways, geometry="way_geometry")

    # TODO: Link seperated ways
    # step 0: search for related name
    # step 1: find wkt head and tail . If self head or tail match other's head or tail, then link them
    result.loc[result]
    result.to_csv(f"ways_level_1.tsv", sep="\t")
    # result.to_file("ways_level.geojson", driver="GeoJSON")

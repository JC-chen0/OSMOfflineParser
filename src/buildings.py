import geopandas
import osmium
from shapely import wkt

from src.util.limit_area import get_relation_polygon_with_overpy, get_limit_relation_geom, prepare_data
from src.util.merging_utils import get_relation_member_data

# %%
wktfab = osmium.geom.WKTFactory()


class BuildingHandler(osmium.SimpleHandler):

    def __init__(self):
        super().__init__()
        self.way_dict = {}
        self.relation_dict = {}

    def relation(self, relation):
        if relation.tags.get("building"):
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        if way.tags.get("building"):
            way_geometry = wkt.loads(wktfab.create_linestring(way))
            self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name") if way.tags.get("name") else "UNKNOWN", "geometry": way_geometry}


class Building:

    def __init__(self, polygon_id, polygon_name, height, level, geometry):
        self.polygon_id = polygon_id
        self.polygon_name = polygon_name
        self.height = height
        self.level = level
        self.geometry = geometry


# %%
if __name__ == "__main__":
    # Config
    input_file = "data/input/country/taiwan-latest.osm.pbf"
    limit_relation_id = "1293250"
    #########
    # Option:
    # 1.
    # limit_area = get_limit_relation_geom(input_file, limit_relation_id)
    # 2.
    limit_area = get_relation_polygon_with_overpy(limit_relation_id)

    building_handler = BuildingHandler()
    building_handler.apply_file(input_file, idx="flex_mem", locations=True)

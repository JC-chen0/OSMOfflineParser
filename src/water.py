from typing import Dict, List

import osmium
from shapely import wkt

wktfab = osmium.geom.WKTFactory()


# WATER_ID -> Using WAY id
class AreaWaterHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        # from way
        self.way_waters = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}
        # from rel
        self.rel_waters = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}

        self.relation_dict: Dict[List[Dict]] = dict()  # RelationID: [{ID,ROLE,TYPE}]

    def area(self, area):
        try:
            if area.tags.get("natural") == "water":
                water_id = area.orig_id()
                water_name = area.tags.get("name")  # create new string object
                water_geometry = wkt.loads(wktfab.create_multipolygon(area))
                if area.from_way():
                    # All area from way is one polygon (len(POLYGON_STR) == 1)
                    water_geometry = list(water_geometry)[0]  # Extract polygon from multipolygon
                    self.append(self.way_waters, water_id, water_name, water_geometry)
                else:
                    self.append(self.rel_waters, water_id, water_name, water_geometry)

        except:
            pass

    def append(self, waters: dict, id, name, geometry):
        waters.get("POLYGON_ID").append(id)
        waters.get("POLYGON_NAME").append(name)
        waters.get("POLYGON_STR").append(geometry)
        waters.get("HOFN_TYPE").append("1")
        waters.get("HOFN_LEVEL").append("1")

    def relation(self, relation):
        if relation.tags.get("natural") == "water":
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"ID": member.ref, "ROLE": member.role, "TYPE": member.type})
                # TODO: If type = relation? how to resolve it


area_handler = AreaWaterHandler()
area_handler.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", idx="flex_mem", locations=True)

# %%
import geopandas
import pandas
relation_dict = pandas.DataFrame()
way_waters = geopandas.GeoDataFrame(area_handler.way_waters, geometry="POLYGON_STR")
rel_waters = geopandas.GeoDataFrame(area_handler.rel_waters, geometry="POLYGON_STR")
way_waters.to_file("data\\output\\water\\way_waters.geojson", driver="GeoJSON")
rel_waters.to_file("data\\output\\water\\rel_waters.geojson", driver="GeoJSON")

# %%
# waters from way doesn't need to modify anything.
result = way_waters

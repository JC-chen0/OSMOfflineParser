import time

import osmium
from typing import Dict, List
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
        self.way_dict: Dict[Dict] = dict()

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

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"ID": way.id, "NAME": way.tags.get("name"), "GEOMETRY": way_geometry}


start_time = time.time()
area_handler = AreaWaterHandler()
area_handler.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", idx="flex_mem", locations=True)
print(f"Get waters data completed, taking {time.time() - start_time}")
# %%
import geopandas
import pandas

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
relation_dict = area_handler.relation_dict
way_dict = area_handler.way_dict
way_waters = geopandas.GeoDataFrame(area_handler.way_waters, geometry="POLYGON_STR")
rel_waters = geopandas.GeoDataFrame(area_handler.rel_waters, geometry="POLYGON_STR")


# %%
def get_relation_member_data(relation_dict: Dict, way_dict: Dict) -> Dict:
    water_rel_members_dict = {"RELATION_ID": [], "WAY_ID": [], "NAME": [], "GEOMETRY": [], "ROLE": [], "TYPE": []}

    for relation_id, members in relation_dict.items():
        for member in members:
            if member.get("ROLE") not in ["inner", "outer"]:
                continue
            way_id = member.get("ID")
            way = way_dict.get(way_id)
            water_rel_members_dict.get("RELATION_ID").append(relation_id)
            water_rel_members_dict.get("WAY_ID").append(way_id)
            water_rel_members_dict.get("NAME").append(way.get("NAME"))
            water_rel_members_dict.get("GEOMETRY").append(way.get("GEOMETRY"))
            water_rel_members_dict.get("ROLE").append(member.get("ROLE"))
            water_rel_members_dict.get("TYPE").append(member.get("TYPE"))
    return water_rel_members_dict


def restructure(relation_member_dict):
    temp = dict()
    for member in relation_member_dict.values():
        relation_id = member.get("RELATION_ID")
        member.pop("RELATION_ID")
        if not temp.get(relation_id, 0):
            temp[relation_id] = []
        temp[relation_id].append(member)
    return temp

# %%
# Prepare data and free memory
relation_member_dict: Dict = get_relation_member_data(relation_dict, way_dict)
relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict, geometry="GEOMETRY")
relation_member_dict = relation_member_data.to_dict("index")
relation_member_dict = restructure(relation_member_dict)
##################################################################
# If way id is being used, use another one.
processed_way_table = list()

for member in relation_member_dict.values():


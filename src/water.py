import time

import osmium
from typing import Dict, List
from shapely import wkt
from shapely.ops import polygonize

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


# way_waters.to_file("data\\output\\water\\way_waters.geojson", driver="GeoJSON")
# rel_waters.to_file("data\\output\\water\\rel_waters.geojson", driver="GeoJSON")

# %%
def is_relation_in_relation(relation_id: str, relation_dict: Dict) -> bool:
    member_list = relation_dict.get(relation_id)
    for member in member_list:
        if member.get("TYPE") == "r":
            return True
    return False


def is_inner_in_relation(relation_id: str, relation_dict: Dict) -> bool:
    member_list = relation_dict.get(relation_id)
    for member in member_list:
        if member.get("ROLE") == "inner":
            return True
    return False


def swap_id_process(rel_waters: geopandas.GeoDataFrame, relation_dict: Dict):
    def get_way_id_in_relation(relation_id: str, relation_dict: Dict):
        member_list = relation_dict.get(relation_id)
        for member in member_list:
            if member.get("TYPE") == "w" and member.get("ROLE") == "outer":
                return member.get("ID")

    waters_result = rel_waters
    waters_result["POLYGON_ID"] = waters_result.apply(lambda row: get_way_id_in_relation(row["POLYGON_ID"], relation_dict), axis=1)
    return waters_result


def extract_island(islands: Dict, rel_waters_level: geopandas.GeoDataFrame, relation_dict: Dict, way_dict: Dict):
    def find_inner_rings(relation_id, relation_dict) -> List[int]:
        inner_rings = list()
        members = relation_dict.get(relation_id)
        for member in members:
            if member.get("ROLE") == "inner":
                inner_rings.append(member.get("ID"))
        return inner_rings

    relation_ids = list(rel_waters_level["POLYGON_ID"])
    inner_rings = [inner_ring for relation_id in relation_ids for inner_ring in find_inner_rings(relation_id, relation_dict)]


    for inner_ring_id in inner_rings:
        print(inner_ring_id)
        inner_ring = way_dict.get(inner_ring_id)
        islands["POLYGON_ID"].append(inner_ring.get("ID"))
        islands["POLYGON_NAME"].append(inner_ring.get("NAME"))
        islands["POLYGON_STR"].append(inner_ring.get("GEOMETRY"))
        islands["HOFN_TYPE"].append("5")
        islands["HOFN_LEVEL"].append("1")
    return islands


def extract_multi_polygon(rel_waters: geopandas.GeoDataFrame, relation_dict: Dict, way_dict: Dict) -> geopandas.GeoDataFrame:
    relation_ids = list(rel_waters["POLYGON_ID"])
    relation_geometries = list(rel_waters["POLYGON_STR"])
    rel_waters_splited = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}
    for relation_id in relation_ids:
        members = relation_dict.get(relation_id)
        for member in members:
            if member.get("ROLE") == "outer":
                polygon = way_dict.get(member.get("ID"))
                rel_waters_splited.get("POLYGON_ID").append(polygon.get("ID"))
                rel_waters_splited.get("POLYGON_NAME").append(polygon.get("NAME"))
                rel_waters_splited.get("POLYGON_STR").append(polygon.get("GEOMETRY"))
                rel_waters_splited.get("HOFN_TYPE").append("1")
                rel_waters_splited.get("HOFN_LEVEL").append("1")
    return geopandas.GeoDataFrame(rel_waters_splited, geometry="POLYGON_STR")


##################################################################
def level1_process(rel_waters_level1: geopandas.GeoDataFrame, relation_dict: Dict):
    return swap_id_process(rel_waters_level1, relation_dict)


def level2_process(islands: dict, rel_waters_level2: geopandas.GeoDataFrame, relation_dict: Dict, way_dict: Dict):
    extract_island(islands, rel_waters_level2, relation_dict, way_dict)
    islands_df = geopandas.GeoDataFrame(islands, geometry="POLYGON_STR")
    islands_df.to_file("islands_df.geojson", driver="GeoJSON")
    # return extract_multi_polygon(rel_waters_level2, relation_dict,way_dict)


def level3_process(islands, rel_waters_level3: geopandas.GeoDataFrame, relation_dict: Dict, way_dict: Dict):
    pass


# def get_result_waters_process(way_waters: geopandas.GeoDataFrame, rel_waters: geopandas.GeoDataFrame, relation_dict: Dict):
# For debugging convenience
rel_waters["PROCESS_LEVEL"] = rel_waters.apply(lambda row: 1 if not is_inner_in_relation(row["POLYGON_ID"], relation_dict) else 2 if not is_relation_in_relation(row["POLYGON_ID"], relation_dict) else 3, axis=1)
rel_waters_level1 = rel_waters[rel_waters["PROCESS_LEVEL"] == 1]
rel_waters_level2 = rel_waters[rel_waters["PROCESS_LEVEL"] == 2]
rel_waters_level3 = rel_waters[rel_waters["PROCESS_LEVEL"] == 3]
# %%
islands = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}
# level process

# Swap relation id with way id
level1_process(rel_waters_level1, relation_dict)
level2_process(islands, rel_waters_level2, relation_dict, way_dict)
level3_process(islands, rel_waters_level3, relation_dict, way_dict)
# %%
processed_rel_waters = pandas.concat([rel_waters_level1, rel_waters_level2, rel_waters_level3])
processed_rel_waters.drop(columns=["POLYGON_LENGTH", "PROCESS_LEVEL"])
result_waters = pandas.concat([way_waters, processed_rel_waters])
result_waters = geopandas.GeoDataFrame(result_waters, geometry="POLYGON_STR")

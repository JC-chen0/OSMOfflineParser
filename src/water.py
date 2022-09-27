import time
from argparse import ArgumentParser

import osmium
import logging
import logging.config
import geopandas
import pandas
import yaml
from typing import Dict, List
from shapely import wkt
from shapely.geometry import LineString, Polygon
from shapely.ops import polygonize, linemerge

from src.utils.merging_utils import is_reverse_needed, reverse_linestring_coords, is_continuous, prepare_data, get_relation_polygon

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
try:
    with open('src/resource/logback.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
        logging.config.dictConfig(config)
except Exception as e:
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Error in Logging Configuration, Using default configs")

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


logging.info("============================================")
logging.info(f"Start extracting waters ...")
file_path = "data\\input\\country\\taiwan-latest.osm.pbf"
logging.info(f"Loading waters data from {file_path},")

start_time = time.time()
area_handler = AreaWaterHandler()
area_handler.apply_file(file_path, idx="flex_mem", locations=True)
logging.debug(f"Get waters data completed, taking {time.time() - start_time}")


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


# Restructure with outer and inner grouping
def restructure(relation_member_dict):
    temp = dict()
    for member in relation_member_dict.values():
        relation_id = member.get("RELATION_ID")
        member.pop("RELATION_ID")
        if not temp.get(relation_id, 0):
            temp[relation_id] = {"outer": [], "inner": []}

        if member.get("ROLE") == "inner":
            temp.get(relation_id).get("inner").append(member)
        elif member.get("ROLE") == "outer":
            temp.get(relation_id).get("outer").append(member)
        else:  # ONLY for debug purpose.
            logging.debug(f"Find way {member.get('WAY_ID')} with invalid ROLE {member.get('ROLE')}.")
    return temp


##################################################################
def inners_extracting(inners: List[Dict], islands: List[Dict]):
    for inner in inners:
        append = {"POLYGON_ID": inner.get("POLYGON_ID"),
                  "POLYGON_NAME": inner.get("POLYGON_NAME"),
                  "POLYGON_STR": inner.get("POLYGON_STR"),
                  "HOFN_TYPE": "5",
                  "HOFN_LEVEL": "1"}
        islands.append(append)


def get_merged(rings: list, polygon_id_used_table: list) -> List[Dict]:
    ############ INLINE FUNCTION ########
    def get_merged_line(ring, merging_candidates: list, merged_indexes: list) -> LineString:
        merging_line = ring.get("GEOMETRY")
        candidate_line = NotImplemented
        merging_index = 0
        while merging_index < len(merging_candidates):
            if merging_index in merged_indexes:
                merging_index += 1
            else:
                candidate = merging_candidates[merging_index]
                candidate_line = candidate.get("GEOMETRY")
                if is_reverse_needed(merging_line, candidate_line):
                    # Reverse the line and do merge with current index again.
                    logging.debug(f"candidate {candidate.get('WAY_ID')} reversed.")
                    candidate_line = reverse_linestring_coords(candidate_line)
                elif is_continuous(merging_line, candidate_line):
                    logging.debug(f"{ring.get('WAY_ID')} merge with {candidate.get('WAY_ID')}")
                    # merge and start new round of iteration.
                    merging_line = linemerge([merging_line, candidate_line])
                    merged_indexes.append(merging_index)
                    merging_index = 0
                else:
                    merging_index += 1
        logging.debug(f"Return {merging_line}")
        return merging_line

    #################################################################################
    # Deep copy with merge candidate.
    merging_candidate = [ring for ring in rings]
    # List[Index] to skip those have been merged
    merged_indexes = []
    result = []
    current_index = 0
    for ring in rings:
        logging.debug(f"{ring.get('WAY_ID')} start doing merge.")
        # Avoid merge with self.
        merged_indexes.append(current_index)
        # Get merged line with current ring.
        merged_line = get_merged_line(ring, merging_candidate, merged_indexes)
        # Choose way_id from merged line.
        for merged_index in merged_indexes:
            way_id = rings[merged_index].get("WAY_ID")
            if way_id not in polygon_id_used_table:
                result.append({'POLYGON_ID': way_id, "POLYGON_NAME": ring.get("NAME"), "POLYGON_STR": merged_line, "HOFN_TYPE": 1, "HOFN_LEVEL": 1})
                polygon_id_used_table.append(way_id)
        current_index += 1
    return result


# %%
relation_dict = area_handler.relation_dict
way_dict = area_handler.way_dict
way_waters = geopandas.GeoDataFrame(area_handler.way_waters, geometry="POLYGON_STR")
rel_waters = geopandas.GeoDataFrame(area_handler.rel_waters, geometry="POLYGON_STR")
# Prepare data and free memory
limit_area = get_relation_polygon("449220")
way_waters = prepare_data(way_waters, limit_area.wkt)
rel_waters = prepare_data(rel_waters, limit_area.wkt)
relation_member_dict: Dict = get_relation_member_data(relation_dict, way_dict)
relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict, geometry="GEOMETRY")
relation_member_dict = relation_member_data.to_dict("index")
relation_member_dict = restructure(relation_member_dict)
# %%
logging.info(f"Merging waters ...")

# Avoid duplicate POLYGON_ID (WAY_ID)
polygon_id_used_table = []

# Results with waters and islands
relation_result = []
islands = []
for relation_id, relation in relation_member_dict.items():
    outers = relation.get("outer")
    if outers:
        outers = get_merged(outers, polygon_id_used_table)
        relation_member_dict[relation_id] = outers
        for outer in outers:
            relation_result.append(outer)
    inners = relation.get("inner")
    if inners:
        inners = get_merged(inners, polygon_id_used_table)
        inners_extracting(inners, islands)

    print("outer and inner merge process completed.")


# %%

def polygonize_with_try_catch(row, remove_list):
    try:
        return Polygon(row["POLYGON_STR"])
    except:
        logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['POLYGON_STR']}, return origin LINESTRING instead")
        remove_id_list.append(row['POLYGON_ID'])
        return row["POLYGON_STR"]


remove_id_list = []
islands = geopandas.GeoDataFrame(islands, geometry="POLYGON_STR")
islands["POLYGON_STR"] = islands.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
islands = islands[~islands.POLYGON_ID.isin(remove_id_list)]

logging.debug("islands extracting done")

remove_id_list = []
waters = geopandas.GeoDataFrame(relation_result, geometry="POLYGON_STR")
waters = pandas.concat([waters, way_waters])
waters["POLYGON_STR"] = waters.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
waters = waters[~waters["POLYGON_ID"].isin(remove_id_list)]
logging.debug("waters data done.")

islands.to_file("data\\output\\water\\islands.geojson", driver="GeoJSON")
waters.to_file("data\\output\\water\\waters.geojson", driver="GeoJSON")
logging.info("Waters process completed.")


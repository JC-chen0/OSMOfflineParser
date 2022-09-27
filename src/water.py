import time
import osmium
import logging
import logging.config
import geopandas
import pandas
import yaml
import traceback
from argparse import ArgumentParser
from typing import Dict, List
from shapely import wkt
from shapely.geometry import LineString, Polygon
from shapely.ops import polygonize, linemerge
from datetime import date
from utils.merging_utils import is_reverse_needed, reverse_linestring_coords, is_continuous, prepare_data, get_relation_polygon

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
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
            if area.tags.get("natural") == "water" or area.tags.get("landuse") == "reservoir" or area.tags.get("waterway") == "riverbank":
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
        if relation.tags.get("natural") == "water" or relation.tags.get("landuse") == "reservoir" or relation.tags.get("waterway") == "riverbank":
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"ID": member.ref, "ROLE": member.role, "TYPE": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"ID": way.id, "NAME": way.tags.get("name"), "GEOMETRY": way_geometry}


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
    def get_merged_line(ring, merging_candidates: list, merged_way_ids: list) -> LineString:
        merging_line = ring.get("GEOMETRY")
        candidate_line = NotImplemented
        merging_index = 0
        while merging_index < len(merging_candidates):
            candidate = merging_candidates[merging_index]
            candidate_line = candidate.get("GEOMETRY")
            candidate_id = candidate.get("WAY_ID")

            if candidate.get('WAY_ID') in merged_way_ids:
                merging_index += 1
            else:
                if is_reverse_needed(merging_line, candidate_line):
                    # Reverse the line and do merge with current index again.
                    logging.debug(f"candidate {candidate_id} reversed.")
                    candidate_line = reverse_linestring_coords(candidate_line)
                elif is_continuous(merging_line, candidate_line):
                    logging.debug(f"{ring.get('WAY_ID')} merge with {candidate_id}")
                    # merge and start new round of iteration.
                    merging_line = linemerge([merging_line, candidate_line])
                    merged_way_ids.append(candidate_id)
                    merging_index = 0
                else:
                    merging_index += 1
        logging.debug(f"Return {merging_line}")
        return merging_line

    #################################################################################
    # Deep copy with merge candidate.
    merging_candidate = [ring for ring in rings]
    # List[Index] to skip those have been merged
    merged_way_ids = []
    result = []
    for ring in rings:
        if ring.get('WAY_ID') not in merged_way_ids:
            logging.debug(f"WAY:{ring.get('WAY_ID')} start doing merge.")
            # Avoid merge with self.
            merged_way_ids.append(ring.get('WAY_ID'))
            # Get merged line with current ring.
            merged_line = get_merged_line(ring, merging_candidate, merged_way_ids)

            # Choose way_id from merged line.
            for merged_id in merged_way_ids:
                if merged_id not in polygon_id_used_table:
                    result.append({'POLYGON_ID': merged_id, "POLYGON_NAME": ring.get("NAME"), "POLYGON_STR": merged_line, "HOFN_TYPE": 1, "HOFN_LEVEL": 1})
                    polygon_id_used_table.append(merged_id)
    return result


def polygonize_with_try_catch(row, remove_list):
    try:
        return Polygon(row["POLYGON_STR"])
    except:
        logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['POLYGON_STR']}, return origin LINESTRING instead")
        remove_id_list.append(row['POLYGON_ID'])
        return row["POLYGON_STR"]


# %%
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("output", type=str, help="Output geojson file path.")
    parser.add_argument("limit_relation", type=str, help="Relation id of limit area.")
    arg = parser.parse_args()
    input_path = arg.input
    output_path = arg.output
    limit_relation_id = arg.limit_relation

    # 1. Get waters data from osm.pbf file
    try:
        with open('src/resource/logback.yaml', 'r') as stream:
            config = yaml.safe_load(stream)
            config.get("handlers").get("info_file_handler")["filename"] = f"logs\\water\\{limit_relation_id}-{date.today()}.info"
            config.get("handlers").get("debug_file_handler")["filename"] = f"logs\\water\\{limit_relation_id}-{date.today()}.debug"
            logging.config.dictConfig(config)
            stream.close()
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG)
        traceback.print_exc()
        logging.debug("Error in Logging Configuration, Using default configs")

    logging.info("============================================")
    logging.info(f"INPUT ARGUMENTS: {arg}")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info("============================================")

    logging.info(f"Start extracting waters ...")
    logging.info(f"[1/4] Loading waters data from {input_path},")

    start_time = time.time()
    area_handler = AreaWaterHandler()
    area_handler.apply_file(input_path, idx="flex_mem", locations=True)
    logging.debug(f"Get waters data completed, taking {time.time() - start_time}")

    # 2. Get data prepared
    logging.info(f"[2/4] Preparing data with intersecting with relation id {limit_relation_id}")
    relation_dict = area_handler.relation_dict
    way_dict = area_handler.way_dict
    way_waters = geopandas.GeoDataFrame(area_handler.way_waters, geometry="POLYGON_STR")
    rel_waters = geopandas.GeoDataFrame(area_handler.rel_waters, geometry="POLYGON_STR")
    # Prepare data and free memory
    limit_area = get_relation_polygon(limit_relation_id)
    way_waters = prepare_data(way_waters, limit_area.wkt, "POLYGON_STR")
    rel_waters = prepare_data(rel_waters, limit_area.wkt, "POLYGON_STR")

    relation_member_dict: Dict = get_relation_member_data(relation_dict, way_dict)
    relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict, geometry="GEOMETRY")
    relation_member_data = prepare_data(relation_member_data, limit_area.wkt, "GEOMETRY")
    relation_member_dict = relation_member_data.to_dict("index")
    relation_member_dict = restructure(relation_member_dict)

    # 3.Merging waters
    logging.info(f"[3/4] Merging waters with outer and inner rings, and extract inner rings as islands.")

    # Avoid duplicate POLYGON_ID (WAY_ID)
    polygon_id_used_table = []

    # Results with waters and islands
    relation_result = []
    islands = []
    for relation_id, relation in relation_member_dict.items():
        logging.debug(f"Relation: {relation_id} doing merge.")
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

        logging.debug("outer and inner merge process completed.")

    # 4. polygonized data and output.
    logging.info("[4/4] Polygonizing data and output.")
    remove_id_list = []
    islands = geopandas.GeoDataFrame(islands, geometry="POLYGON_STR")
    islands["POLYGON_STR"] = islands.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
    islands = islands[~islands.POLYGON_ID.isin(remove_id_list)]
    logging.debug(f"Remove {remove_id_list} from islands due to unpolygonizable issue.")
    logging.debug("islands polygonized done")

    remove_id_list = []
    waters = geopandas.GeoDataFrame(relation_result, geometry="POLYGON_STR")
    waters = pandas.concat([waters, way_waters])
    waters["POLYGON_STR"] = waters.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
    waters = waters[~waters["POLYGON_ID"].isin(remove_id_list)]
    logging.debug(f"Remove {remove_id_list} from waters due to unpolygonizable issue.")
    logging.debug("waters polygonized done.")

    islands.to_file(f"{output_path}\\islands.geojson", driver="GeoJSON")
    waters.to_file(f"{output_path}\\waters.geojson", driver="GeoJSON")
    logging.info("Waters process completed.")

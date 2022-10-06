import math
import os
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
import area
from src.enum.mcc import National
from src.util.limit_area import get_relation_polygon_with_overpy
from src.util.merging_utils import is_reverse_needed, reverse_linestring_coords, is_continuous, prepare_data, linemerge_by_wkt
from src.enum.hofn_type import HofnType

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
wktfab = osmium.geom.WKTFactory()


# RING_ID -> Using WAY id
class RingHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode):
        super().__init__()
        # from way
        self.way_rings = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': []}
        # from rel
        self.rel_rings = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': []}

        self.relation_dict: Dict[List[Dict]] = dict()  # RelationID: [{ID,ROLE,TYPE}]
        self.way_dict: Dict[Dict] = dict()
        self.mode = mode
        self.tags = tags

    def area(self, area):
        try:
            if any([area.tags.get(key) in value if type(value) == list else area.tags.get(key) == value for key, value in self.tags.items()]):
                ring_id = area.orig_id()
                ring_name = area.tags.get("name")  # create new string object
                ring_geometry = wkt.loads(wktfab.create_multipolygon(area))
                if area.from_way():
                    # All area from way is one polygon (len(POLYGON_STR) == 1)
                    ring_geometry = list(ring_geometry)[0]  # Extract polygon from multipolygon
                    self.append(self.way_rings, ring_id, ring_name, ring_geometry)
                else:
                    self.append(self.rel_rings, ring_id, ring_name, ring_geometry)

        except:
            pass

    def append(self, rings: dict, id, name, geometry):
        rings.get("POLYGON_ID").append(id)
        rings.get("POLYGON_NAME").append(name)
        rings.get("POLYGON_STR").append(geometry)
        rings.get("HOFN_TYPE").append(HofnType[self.mode].value)
        rings.get("ROAD_LEVEL").append("0")

    def relation(self, relation):
        if any([relation.tags.get(key) == value for key, value in self.tags.items()]):
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"ID": member.ref, "ROLE": member.role, "TYPE": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"ID": way.id, "NAME": way.tags.get("name"), "GEOMETRY": way_geometry}


def get_relation_member_data(relation_dict: Dict, way_dict: Dict) -> Dict:
    ring_rel_members_dict = {"RELATION_ID": [], "WAY_ID": [], "NAME": [], "GEOMETRY": [], "ROLE": [], "TYPE": []}

    for relation_id, members in relation_dict.items():
        if relation_id in [4152133]:
            print("")
        for member in members:
            if member.get("ROLE") not in ["inner", "outer"]:
                continue
            way_id = member.get("ID")
            way = way_dict.get(way_id)
            if way:
                ring_rel_members_dict.get("RELATION_ID").append(relation_id)
                ring_rel_members_dict.get("WAY_ID").append(way_id)
                ring_rel_members_dict.get("NAME").append(way.get("NAME"))
                ring_rel_members_dict.get("GEOMETRY").append(way.get("GEOMETRY"))
                ring_rel_members_dict.get("ROLE").append(member.get("ROLE"))
                ring_rel_members_dict.get("TYPE").append(member.get("TYPE"))
            else:
                logging.debug(f"{way_id} cannot be found in way dict, please check.")
    return ring_rel_members_dict


# Restructure with outer and inner grouping
def restructure(relation_member_dict):
    temp = dict()
    for member in relation_member_dict.values():
        relation_id = member.get("RELATION_ID")
        if relation_id == 4152133:
            print("")
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
                  "ROAD_LEVEL": "0"}
        islands.append(append)


def get_merged_rings(rings: list, polygon_id_used_table: list, merged_way_ids, mode) -> List[Dict]:
    ############ INLINE FUNCTION ########
    def get_merged_line(ring, merging_candidates: list, merged_way_ids: list, current_merged_ids) -> LineString:
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
                    merging_line = linemerge_by_wkt(merging_line, candidate_line)
                    current_merged_ids.append(candidate_id)
                    merged_way_ids.append(candidate_id)
                    merging_index = 0
                else:
                    merging_index += 1
        logging.debug(f"Return {merging_line}")
        return merging_line

    #################################################################################
    # Deep copy with merge candidate.
    merging_candidate = [ring for ring in rings]
    result = []
    for ring in rings:
        current_merged_ids = []
        if ring.get('WAY_ID') not in merged_way_ids:
            logging.debug(f"WAY:{ring.get('WAY_ID')} start doing merge.")
            # Avoid merge with self.
            merged_way_ids.append(ring.get('WAY_ID'))
            # Get merged line with current ring.
            current_merged_ids.append(ring.get('WAY_ID'))
            merged_line = get_merged_line(ring, merging_candidate, merged_way_ids, current_merged_ids)

            # Choose way_id from merged line.
            for merged_id in current_merged_ids:
                if merged_id not in polygon_id_used_table:
                    result.append({'POLYGON_ID': merged_id, "POLYGON_NAME": ring.get("NAME"), "POLYGON_STR": merged_line, "HOFN_TYPE": HofnType[mode].value, "ROAD_LEVEL": 0})
                    polygon_id_used_table.append(merged_id)
                    break
                else:
                    logging.debug(f"{merged_id} has been used, change another polygon id")
    return result


def polygonize_with_try_catch(row, remove_list):
    try:
        return Polygon(row["POLYGON_STR"])
    except:
        logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['POLYGON_STR']}, return origin LINESTRING instead")
        remove_list.append(row['POLYGON_ID'])
        return row["POLYGON_STR"]


def remove_within_outer(rings: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    def is_within(row, compare: geopandas.GeoSeries, current_index):
        try:
            polygons = compare.drop(current_index)
            within = polygons.contains(row["POLYGON_STR"])
            if True in list(within):
                logging.debug(f"{row['POLYGON_ID']} will be removed due to within other polygon.")
                return 1
            return 0
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot do contains.")
            traceback.print_exc()

    start_time = time.time()
    compare = rings["POLYGON_STR"]
    for index, row in rings.iterrows():
        rings.at[index, "within"] = is_within(row, compare, index)
    extract = rings[rings["within"] != 1]
    logging.debug(f"remove within outer, taking {time.time() - start_time}")
    extract = extract.drop(columns=["within"])
    return extract


def remove_over_intersection_outer(rings: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    def is_over_intersect(row, compare: geopandas.GeoSeries, current_index):
        try:
            geometry = row["POLYGON_STR"]
            polygons = compare.drop(current_index)
            intersects = polygons.intersects(geometry)
            if True in list(intersects):
                true_indexes = [i for i, x in intersects.iteritems() if x]
                for index in true_indexes:
                    intersect_polygon = geometry.difference(polygons[index])
                    cover_percentage = (intersect_polygon.area / geometry.area) * 100
                    if cover_percentage < 3:
                        print(f"{row['POLYGON_ID']} will be removed due to over_intersection")
                        return 1
            return 0
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot do intersect.")
            traceback.print_exc()

    start_time = time.time()
    compare = rings["POLYGON_STR"]
    rings["over_intersect"] = 0
    for index, row in rings.iterrows():
        rings.at[index, "over_intersect"] = is_over_intersect(row, compare, index)
    extract = rings[rings["over_intersect"] != 1]
    extract = extract.drop(columns=["over_intersect"])
    print(f"remove within outer, taking {time.time() - start_time}")
    return extract


# %%
def main(input_path, output_path, nation, limit_relation_id, mode, tags, debugging=False):
    DEBUGGING = debugging
    IS_VILLAGE = True if mode == "village" else False
    IS_WATER = True if mode == "water" else False
    island_output_path = f"data/output/island/{nation}"
    if not os.path.isdir(island_output_path):
        os.makedirs(island_output_path)

    # 1. Get coastlines data from osm.pbf file
    logging.info(f"Start extracting rings ...")
    logging.info(f"[1/4] Loading data from {input_path}, tags: {tags}")

    start_time = time.time()
    area_handler = RingHandler(tags, mode)
    area_handler.apply_file(input_path, idx="flex_mem", locations=True)
    logging.debug(f"Get data completed, taking {time.time() - start_time} seconds")

    # 2. Get data prepared
    logging.info(f"[2/4] Preparing data with intersecting with relation id {limit_relation_id}")
    relation_dict = area_handler.relation_dict
    way_dict = area_handler.way_dict
    way_rings = geopandas.GeoDataFrame(area_handler.way_rings, geometry="POLYGON_STR")
    rel_rings = geopandas.GeoDataFrame(area_handler.rel_rings, geometry="POLYGON_STR")

    # Prepare data and free memory
    limit_area = get_relation_polygon_with_overpy(limit_relation_id)
    way_rings = prepare_data(way_rings, limit_area.wkt, "POLYGON_STR")
    rel_rings = prepare_data(rel_rings, limit_area.wkt, "POLYGON_STR")

    relation_member_dict: Dict = get_relation_member_data(relation_dict, way_dict)
    relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict, geometry="GEOMETRY")
    relation_member_data = prepare_data(relation_member_data, limit_area.wkt, "GEOMETRY")
    relation_member_dict = relation_member_data.to_dict("index")
    relation_member_dict = restructure(relation_member_dict)

    # 3.Merging rings
    logging.info(f"[3/4] Merging rings with outer and inner rings, and extract inner rings as islands.")

    # Avoid duplicate POLYGON_ID (WAY_ID)
    polygon_id_used_table = []

    # Results with rings and islands
    relation_result = []
    islands = []
    # List[Index] to skip those have been merged
    merged_way_ids = []
    for relation_id, relation in relation_member_dict.items():
        logging.debug(f"Relation: {relation_id} doing merge.")
        outers = relation.get("outer")
        if relation_id == 4152133:
            print("")
        if outers:
            outers = get_merged_rings(outers, polygon_id_used_table, merged_way_ids, mode)
            relation_member_dict[relation_id] = outers
            for outer in outers:
                relation_result.append(outer)

        if IS_WATER:
            inners = relation.get("inner")
            if inners:
                inners = get_merged_rings(inners, polygon_id_used_table, merged_way_ids, "island")
                inners_extracting(inners, islands)

        logging.debug("outer and inner merge process completed.")

    # 4. polygonized data and output.

    logging.info("[4/4] Polygonizing data and output.")
    if IS_WATER:
        remove_id_list = []
        islands = geopandas.GeoDataFrame(islands, geometry="POLYGON_STR")
        islands["POLYGON_STR"] = islands.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
        islands = islands[~islands.POLYGON_ID.isin(remove_id_list)]
        logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
        logging.debug("islands polygonized done")
        islands = islands.drop(islands[islands.POLYGON_STR.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < 200 * 200].index)
        if DEBUGGING:
            islands.to_file(f"{island_output_path}/island.geojson", driver="GeoJSON")
        else:
            islands.to_csv(f"{island_output_path}/island.tsv", sep="\t", index=False)

    remove_id_list = []
    rings = geopandas.GeoDataFrame(relation_result, geometry="POLYGON_STR")
    rings = pandas.concat([rings, way_rings])
    rings["POLYGON_STR"] = rings.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
    rings = rings[~rings["POLYGON_ID"].isin(remove_id_list)]
    logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
    logging.debug("rings polygonized done.")
    rings = rings.drop(rings[rings.POLYGON_STR.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < 200 * 200].index)
    # if IS_VILLAGE:
    #     rings = remove_within_outer(rings)
    #     rings = remove_over_intersection_outer(rings)

    if DEBUGGING:
        rings.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON")
    else:
        rings.to_csv(f"{output_path}/{mode}.tsv", sep="\t", index=False)
    logging.info("rings process completed.")

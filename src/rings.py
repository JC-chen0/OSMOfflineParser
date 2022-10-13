import math
import os
import sys
import time
import osmium
import logging
import logging.config
import geopandas
import pandas
from typing import Dict, List
from shapely import wkt
from src.util.limit_area import get_relation_polygon_with_overpy, prepare_data, get_limit_relation_geom
from src.util.merging_utils import get_relation_member_data, restructure, inners_extracting, get_merged_rings, polygonize_with_try_catch, remove_over_intersection_outer
from src.enum.hofn_type import HofnType

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
wktfab = osmium.geom.WKTFactory()


# RING_ID -> Using WAY id
class RingHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode):
        super().__init__()
        # from way
        self.way_rings = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'geometry': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': []}
        # from rel
        self.rel_rings = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'geometry': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': []}

        self.relation_dict: Dict[List[Dict]] = dict()  # RelationID: [{ID,ROLE,TYPE}]
        self.way_dict: Dict[Dict] = dict()
        self.mode = mode
        self.tags = tags

    def area(self, area):
        try:
            if any([area.tags.get(key) in value if type(value) == list else area.tags.get(key) == value for key, value in self.tags.items()]):
                ring_id = area.orig_id()
                ring_name = area.tags.get("name") if area.tags.get("name") else "UNKNOWN"  # create new string object
                ring_geometry = wkt.loads(wktfab.create_multipolygon(area))
                if area.from_way():
                    # All area from way is one polygon (len(geometry) == 1)
                    ring_geometry = list(ring_geometry)[0]  # Extract polygon from multipolygon
                    self.append(self.way_rings, ring_id, ring_name, ring_geometry)
                else:
                    self.append(self.rel_rings, ring_id, ring_name, ring_geometry)

        except:
            pass

    def append(self, rings: dict, id, name, geometry):
        rings.get("POLYGON_ID").append(id)
        rings.get("POLYGON_NAME").append(name)
        rings.get("geometry").append(geometry)
        rings.get("HOFN_TYPE").append(HofnType[self.mode].value)
        rings.get("ROAD_LEVEL").append("0")

    def relation(self, relation):
        if any([relation.tags.get(key) == value for key, value in self.tags.items()]):
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name") if way.tags.get("name") else "UNKNOWN", "geometry": way_geometry}


##################################################################

# %%
def main(input_path, output_path, nation, limit_relation_id, mode, tags, DEBUGGING=False):
    IS_VILLAGE = True if mode == "village" else False
    IS_WATER = True if mode == "water" else False
    island_output_path = f"data/output/island/{nation}"
    if not os.path.isdir(island_output_path):
        os.makedirs(island_output_path)
    #######################################################################################
    # 1. Get coastlines data from osm.pbf file
    logging.info(f"Start extracting rings ...")
    logging.info(f"[1/4] Loading data from {input_path}, tags: {tags}")

    start_time = time.time()
    area_handler = RingHandler(tags, mode)
    area_handler.apply_file(input_path, idx="flex_mem", locations=True)
    logging.debug(f"Get data completed, taking {time.time() - start_time} seconds")
    #######################################################################################
    # 2. Get data prepared
    logging.info(f"[2/4] Preparing data with intersecting with relation id {limit_relation_id}")
    relation_dict = area_handler.relation_dict
    way_dict = area_handler.way_dict
    way_rings = geopandas.GeoDataFrame(area_handler.way_rings)
    rel_rings = geopandas.GeoDataFrame(area_handler.rel_rings)

    # Prepare data and free memory
    # limit_area = get_relation_polygon_with_overpy(limit_relation_id)
    limit_area = get_limit_relation_geom(input_path, limit_relation_id)
    way_rings = prepare_data(way_rings, limit_area.wkt)
    rel_rings = prepare_data(rel_rings, limit_area.wkt)

    relation_member_dict: Dict = get_relation_member_data(relation_dict, way_dict, tags=["outer", "inner", ""])
    relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict)
    relation_member_data = prepare_data(relation_member_data, limit_area.wkt)
    relation_member_dict = relation_member_data.to_dict("index")
    relation_member_dict = restructure(relation_member_dict)
    #######################################################################################
    # 3.Merging rings
    logging.info(f"[3/4] Merging rings with outer and inner rings, and extract inner rings as islands.")

    # Avoid duplicate POLYGON_ID (WAY_ID)
    polygon_id_used_table = []

    # Results with rings and islands
    relation_result = []
    islands = []

    for relation_id, relation in relation_member_dict.items():
        logging.debug(f"Relation: {relation_id} doing merge.")

        outers = relation.get("outer")
        if outers:
            outers = get_merged_rings(outers, polygon_id_used_table, mode)
            relation_member_dict[relation_id] = outers
            for outer in outers:
                relation_result.append(outer)

        if IS_WATER:
            inners = relation.get("inner")
            if inners:
                inners = get_merged_rings(inners, polygon_id_used_table, "island")
                inners_extracting(inners, islands)

        logging.debug("outer and inner merge process completed.")
    #######################################################################################
    # 4. polygonized data and output.
    logging.info("[4/4] Polygonizing data and output.")
    if IS_WATER:
        remove_id_list = []
        islands = geopandas.GeoDataFrame(islands)
        islands["geometry"] = islands.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
        islands = islands[~islands.POLYGON_ID.isin(remove_id_list)]
        logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
        logging.debug("islands polygonized done")
        islands = islands.drop(islands[islands.geometry.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < 200 * 200].index)
        if DEBUGGING:
            islands.to_file(f"{island_output_path}/island.geojson", driver="GeoJSON", encoding="utf-8")
        else:
            islands.to_csv(f"{island_output_path}/island.tsv", sep="\t", index=False)

    remove_id_list = []
    rings = geopandas.GeoDataFrame(relation_result, geometry="geometry")
    rings = pandas.concat([rings, way_rings])
    rings["geometry"] = rings.apply(lambda row: polygonize_with_try_catch(row, remove_id_list), axis=1)
    rings = rings[~rings["POLYGON_ID"].isin(remove_id_list)]
    logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
    logging.debug("rings polygonized done.")
    rings = rings.drop(rings[rings.geometry.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < 200 * 200].index)
    if IS_VILLAGE:
        # rings = remove_within_outer(rings)
        rings = remove_over_intersection_outer(rings)

    if DEBUGGING:
        rings.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")
    else:
        rings.to_csv(f"{output_path}/{mode}.tsv", sep="\t", index=False)
        rings.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")

    logging.info("rings process completed.")
    sys.exit(0)

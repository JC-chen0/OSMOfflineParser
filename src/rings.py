import math
import os
import sys
import time
from itertools import repeat

import numpy
import osmium
import logging
import logging.config
import geopandas
import pandas
import multiprocessing
from src.util.merging_utils import RingUtils, MPUtils
from src.util.limit_area import LimitAreaUtils
from typing import Dict, List
from shapely import wkt, ops
from src.enum.hofn_type import HofnType

# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
pandas.options.mode.chained_assignment = None  # default='warn'
wktfab = osmium.geom.WKTFactory()
cpu_count = int(numpy.where(multiprocessing.cpu_count() > 20, 20, multiprocessing.cpu_count() - 1))


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

    # Tags: 1. Value 2. list 3. "" (purely take all the tags)
    def area(self, area):
        try:
            if any([area.tags.get(key) in value if type(value) == list else area.tags.get(key) == value if value != "" else area.tags.get("key") for key, value in self.tags.items()]):
                ring_id = area.orig_id()
                ring_name = area.tags.get("name") if area.tags.get("name") else "UNKNOWN"  # create new string object
                ring_geometry = wkt.loads(wktfab.create_multipolygon(area))
                if ring_geometry.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 > 200 * 200:
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

    # Tags: 1. Value 2. list 3. "" (purely take all the tags)
    def relation(self, relation):
        if any([relation.tags.get(key) in value
                if type(value) == list else relation.tags.get(key) == value
        if value != "" else relation.tags.get("key")
                for key, value in self.tags.items()]):
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        try:
            processed = list(ops.polygonize(way_geometry))[0]
            if processed.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 > 200 * 200:
                self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name") if way.tags.get("name") else "UNKNOWN", "geometry": way_geometry}
        except:
            # If cannot be polygonized, it should be a slice of linestring, just add to merge it later.
            self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name") if way.tags.get("name") else "UNKNOWN", "geometry": way_geometry}


##################################################################

# %%
def main(input_path, output_path, nation, limit_relation_id, mode, tags, DEBUGGING=False, ALL_OFFLINE=False):
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

    # Prepare data with limit area and free memory
    del area_handler
    if ALL_OFFLINE:
        logging.debug("Detect all offline mode on, using offline file to load limit area")
        limit_area = LimitAreaUtils.get_limit_relation_geom(input_path, limit_relation_id)
    else:
        logging.debug("Detect all offline mode off, using api to load limit area")
        limit_area = LimitAreaUtils.get_relation_polygon_with_overpy(limit_relation_id)

    logging.info("Preparing way data.")
    pool = multiprocessing.Pool(cpu_count)
    way_sub_rings = numpy.array_split(way_rings, cpu_count)
    result_list = pool.starmap(LimitAreaUtils.prepare_data, zip(way_sub_rings, repeat(limit_area.wkt)))
    pool.close()
    way_rings = geopandas.GeoDataFrame()
    for result in result_list:
        way_rings = pandas.concat([way_rings, result])
    way_rings.to_file(f"{output_path}/way_water.geojson", driver="GeoJSON", encoding="utf-8")

    logging.info("Preparing relation data.")
    # Prepare relation data.
    relation_members: list = RingUtils.get_relation_member_data(relation_dict, way_dict, tags=["outer", "inner", ""])
    # Intersect with limit area to limit geometries.
    relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_members)
    relation_member_data = LimitAreaUtils.prepare_data(relation_member_data, limit_area.wkt)
    # Restructure as dict for iteration.
    relation_member_dict = relation_member_data.to_dict("index")
    relation_member_dict = RingUtils.restructure(relation_member_dict)
    #######################################################################################
    # 3.Merging rings
    logging.info(f"[3/4] Merging rings with outer and inner rings, and extract inner rings as islands.")
    pool = multiprocessing.Pool(cpu_count)
    manager = multiprocessing.Manager()
    polygon_id_used_table = manager.list()

    # Avoid duplicate POLYGON_ID (WAY_ID)
    polygon_id_used_table += list(way_rings["POLYGON_ID"].values)

    # Results with rings and islands
    relation_result = manager.list()
    islands = manager.list()

    relation_member_sub_dicts = [item for item in MPUtils.chunks(relation_member_dict, int(len(relation_member_dict) / cpu_count))]
    pool.starmap(RingUtils.get_rings_merged_results, zip(relation_member_sub_dicts, repeat(relation_result), repeat(islands), repeat(polygon_id_used_table), repeat(mode)))
    pool.close()
    islands = list(islands)
    relation_result = list(relation_result)

    logging.debug("outer and inner merge process completed.")
    #######################################################################################
    # 4. polygonized data and output.
    logging.info("[4/4] Polygonizing data and output.")
    if IS_WATER:
        remove_id_list = []
        islands = geopandas.GeoDataFrame(islands)
        islands["geometry"] = islands.apply(lambda row: RingUtils.polygonize_with_try_catch(row, remove_id_list), axis=1)
        islands = islands[~islands.POLYGON_ID.isin(remove_id_list)]
        logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
        logging.debug("islands polygonized done")
        islands = islands[islands.geometry.area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 > 200 * 200]
        if DEBUGGING:
            islands.to_file(f"{island_output_path}/island.geojson", driver="GeoJSON", encoding="utf-8")
        else:
            islands.to_csv(f"{island_output_path}/island.tsv", sep="\t", index=False)
            islands.to_file(f"{island_output_path}/island.geojson", driver="GeoJSON", encoding="utf-8")

    remove_id_list = []
    rings = geopandas.GeoDataFrame(relation_result)
    rings = pandas.concat([rings, way_rings])
    rings["geometry"] = rings.apply(lambda row: RingUtils.polygonize_with_try_catch(row, remove_id_list), axis=1)
    rings = rings[~rings["POLYGON_ID"].isin(remove_id_list)]
    logging.debug(f"Remove {remove_id_list}  due to unpolygonizable issue.")
    logging.debug("rings polygonized done.")

    rings = rings[(rings["geometry"].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180) > 200 * 200]


    if DEBUGGING:
        rings.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")
    else:
        rings.to_csv(f"{output_path}/{mode}.tsv", sep="\t", index=False)
        rings.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")

    logging.info("rings process completed.")
    sys.exit(0)

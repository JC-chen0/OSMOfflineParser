import logging.config
import logging
import math
import multiprocessing
import sys
import time
import traceback
import os
import geopandas
import numpy
import osmium
import pandas
import shapely.ops
from shapely import wkt, ops
from src.enum.hofn_type import HofnType
from src.util.limit_area import LimitAreaUtils
from src.util.merging_utils import LineUtils, MPUtils
from src.enum.tag import Tag

# %%
wkt_factory = osmium.geom.WKTFactory()
cpu_count = multiprocessing.cpu_count() - 1 if multiprocessing.cpu_count() < 20 else 20


class LineHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode, level=None):
        super().__init__()
        self.lines = {'POLYGON_ID': [], 'POLYGON_NAME': [], "HOFN_TYPE": [], "ROAD_LEVEL": [], 'geometry': []}
        self.tags = tags
        self.mode = mode
        self.level = level

    # Tags: 1. Value 2. list 3. "" (purely take all the tags)
    def way(self, w):
        line_id = w.id
        line_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        if any([w.tags.get(key) in value if type(value) == list else w.tags.get(key) == value if value != "" else w.tags.get("key")
                for key, value in self.tags.items()]):
            line = wkt.loads(wkt_factory.create_linestring(w))
            level = self.level.get(w.tags.get(self.mode), False) if self.level else 0  # For LEVEL_DICT-need way
            if level is not False:
                try:
                    self.append_line_attribute(self.lines, line_id, line_name, line, level)
                except Exception as e:
                    traceback.print_exc()

    def append_line_attribute(self, attributes: dict, line_id: str, name, geometry, level):
        attributes["POLYGON_ID"].append(line_id)
        attributes["POLYGON_NAME"].append(name)
        attributes["geometry"].append(geometry)
        attributes["HOFN_TYPE"].append(HofnType[self.mode].value)
        attributes["ROAD_LEVEL"].append(level)


def main(input_path, output_path, nation, limit_relation_id, mode, tags, DEBUGGING=False, DIVIDE=None, LEVEL_DICT=None, ALL_OFFLINE=True):
    IS_LEVEL = True if LEVEL_DICT else False
    IS_RING = True if mode in ["coastline"] else False
    IS_FERRY = True if mode in ["ferry"] else False
    ###############################################################################################
    # 1. GET DATA
    if not DIVIDE:
        logging.info("[1/2] Prepare line data from osm.pbf file.")
        logging.info(f"Reading file from {input_path}")
        line_handler = LineHandler(tags, mode, LEVEL_DICT)
        line_handler.apply_file(input_path, idx="flex_mem", locations=True)
        line_df = geopandas.GeoDataFrame(line_handler.lines)

        del line_handler

        line_df.to_file(f"{output_path}/unmerged.geojson", driver="GeoJSON", index=False, encoding="utf-8")

        if ALL_OFFLINE:
            logging.debug("Detect all offline mode on, using offline file to load limit area")
            limit_area = LimitAreaUtils.get_limit_relation_geom(input_path, limit_relation_id)
        else:
            logging.debug("Detect all offline mode off, using api to load limit area")
            limit_area = LimitAreaUtils.get_relation_polygon_with_overpy(limit_relation_id)
        logging.debug("Load limit area geometry completed. Start intersection.")
        tmp = geopandas.read_file(f"{output_path}/unmerged.geojson")
        data = LimitAreaUtils.prepare_data(tmp, limit_area.wkt)
        ###############################################################################################
        # 2. MERGE ALL LINE
        logging.info("[2/2] Merge all the line.")
        start_time = time.time()
        levels = Tag.get_levels(mode, LEVEL_DICT) if IS_LEVEL else [0]
        unmergeds = [data[data["ROAD_LEVEL"] == level] for level in levels]
        result = LineUtils.merge_level_ways(unmergeds, cpu_count)
        #############################################################################
        # After merging, we need some operations with difference mode
        # ONLY those linestring being ringed need to filter with area threshold
        if IS_RING:
            result = LineUtils.filter_small_island(result, area_threshold=40000)
        merged = geopandas.GeoDataFrame(result)
        if IS_FERRY:
            merged["geometry"] = merged.geometry.apply(lambda geometry: geometry.buffer(15 / 6371000 / math.pi * 180))
        merged.to_file(f"{output_path}/merged.geojson", driver="GeoJSON", index=False, encoding="utf-8")

        logging.info(f"Merging completed, taking {time.time() - start_time} seconds")
    ###########################################################################################
    # [OPTIONAL] 3. re-merge and DIVIDE
    if DIVIDE:
        threshold = 100.0
        logging.info(f"[OPTIONAL]Extracting geometry, re-merge and DIVIDE with threshold {threshold} km.")
        logging.info(f"Divide and re-merge POLYGON_ID: {DIVIDE}")
        if os.path.exists(f"{output_path}/merged.geojson"):
            merged = geopandas.read_file(f"{output_path}/merged.geojson")
        else:
            merged = geopandas.read_file(f"{output_path}/{mode}.tsv", sep="\t")
        divide_result_dict = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': [], 'geometry': []}
        # Find all the line which length is larger than [user-set] km, DIVIDE it later.
        lengthy_geometry_ids = DIVIDE
        merged_result = []
        # Divide all the lengthy (LENGTH >600km) geometry
        logging.debug("Start re-merge and DIVIDE.")

        unmerged = geopandas.read_file(f"{output_path}/unmerged.geojson")

        for lengthy_id in lengthy_geometry_ids:
            logging.debug(f"{lengthy_id} is being re-merged and divided.")
            lengthy_geom = list(merged.loc[merged["POLYGON_ID"] == int(lengthy_id)]["geometry"])[0]
            lengthy = LimitAreaUtils.prepare_data(unmerged, lengthy_geom.wkt)
            logging.debug("Prepare completed. Start merge.")
            lengthy_dict = lengthy.set_index(lengthy["POLYGON_ID"]).to_dict('index')
            lengthy_merged_result = LineUtils.get_merged_and_divided(lengthy_dict, lengthy_geom, 100.0)
            merged_result += lengthy_merged_result

        merged = merged[~merged["POLYGON_ID"].isin(DIVIDE)]
        merged = pandas.concat([geopandas.GeoDataFrame(merged_result), merged])



        logging.info("Divide and re-merge completed.")

    #####################################################################################
    # OUTPUT
    if DEBUGGING:
        merged.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8", index=False)
    else:
        merged.to_csv(f"{output_path}/{mode}.tsv", sep="\t", index=False)
        merged.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8", index=False)

    logging.info("==================================")
    logging.info(f"Output file to: {output_path}/{mode}.geojson") if not DEBUGGING else logging.debug(f"Output file to: {output_path}/{mode}.tsv")
    sys.exit(0)

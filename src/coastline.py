import logging.config
import logging
import math
import time
import traceback
import os
from argparse import ArgumentParser
from copy import deepcopy
from datetime import date
from typing import Dict

import geopandas
import osmium
import pandas
import yaml
from geopandas import GeoDataFrame
from shapely import wkt
from shapely.ops import polygonize

from src.enum.hofntype import HofnType
from src.util.limit_area import get_limit_relation_geom, get_relation_polygon_with_overpy
from util.merging_utils import lonlat_length_in_km, prepare_data, get_merged_and_divided_by_threshold, read_file_and_rename_geometry, merge_with_candidates, merge_with_candidates_dict

# %%
wkt_factory = osmium.geom.WKTFactory()


class CoastlineHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode):
        super().__init__()
        self.coastlines = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], "HOFN_TYPE": [], "HOFN_LEVEL": []}
        self.tags = tags
        self.mode = mode

    def way(self, w):
        coastline_id = w.id
        coastline_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        if any([w.tags.get(key) == value for key, value in self.tags.items()]):
            coastline = wkt.loads(wkt_factory.create_linestring(w))
            try:
                self.append_coastline_attribute(self.coastlines, coastline_id, coastline_name, coastline)
            except Exception as e:
                traceback.print_exc()

    def append_coastline_attribute(self, attributes: dict, coastline_id: str, name, geometry):
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        attributes["POLYGON_ID"].append(coastline_id)
        attributes["POLYGON_NAME"].append(name)
        attributes["POLYGON_STR"].append(geometry)
        attributes["HOFN_TYPE"].append(HofnType[mode].value)
        attributes["HOFN_LEVEL"].append(1)


def filter_small_island(merged: geopandas.GeoDataFrame, area_threshold: int):
    #  filter the small island, where there is no people
    small_island_list = []

    def get_small_island_list(row, small_island_list: list):
        try:
            if list(polygonize(row["POLYGON_STR"]))[0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
                small_island_list.append(row["POLYGON_ID"])
                return row
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot be polygonized.")

    filtered = merged.apply(lambda row: get_small_island_list(row, small_island_list), axis=1)
    filtered = merged[~merged["POLYGON_ID"].isin(small_island_list)]
    return filtered


# %%
if __name__ == "__main__":
    DEBUGGING = True

    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("output", type=str, help="Output geojson file path.")
    parser.add_argument("limit_relation", type=str, help="Relation id of limit area.")
    parser.add_argument("--divide", const=True, default=False, nargs="?")  # Set as a flag
    parser.add_argument("--mode", type=str, help="Process mode, Output file name")
    parser.add_argument("--tags", type=str, help="format: tag_name1 search_value1 tag_name2 search_value2 ...", nargs="+")

    if DEBUGGING:
        args = ["D:\\projects\\osm_offline_module\\data\\input\\country\\taiwan-latest.osm.pbf", "D:\\projects\\osm_offline_module\\data\\output\\coastline\\Taiwan", "449220", True, ["natural", "coastline"], "coastline"]
        input_path = args[0]
        output_path = args[1]
        limit_relation_id = args[2]
        divide = args[3]
        tags = {args[4][0]: args[4][1]}
        mode = args[5]
    else:
        args = parser.parse_args()
        input_path = args.input
        output_path = args.output
        limit_relation_id = args.limit_relation
        divide = args.divide
        mode = args.mode
        tags = {}
        tmp = 0
        while tmp < len(args.tags) - 1:
            tag = args.tags[tmp]
            value = args.tags[tmp + 1]
            tags[args.tags[tmp]] = args.tags[tmp + 1]
            tmp += 2

    try:
        with open('src/resource/logback.yaml', 'r') as stream:
            config = yaml.safe_load(stream)
            config.get("handlers").get("info_file_handler")["filename"] = f"logs\\{mode}\\{limit_relation_id}-{date.today()}.info"
            config.get("handlers").get("debug_file_handler")["filename"] = f"logs\\{mode}\\{limit_relation_id}-{date.today()}.debug"
            logging.config.dictConfig(config)
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG)
        traceback.print_exc()
        logging.debug("Error in Logging Configuration, Using default configs")

    # 1. Get coastlines data from osm.pbf file
    logging.info("============================================")
    logging.info(f"INPUT ARGUMENTS: {args}")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info(f"MODE: {mode}")
    logging.info(f"SEARCH TAG WITH VALUE: {tags}")
    logging.info(f"RE-MERGING AND DIVIDING MODE: {'ON' if divide else 'OFF'}")
    logging.info("============================================")

    logging.info("[1/3] Prepare coastline data from osm.pbf file.")
    logging.info(f"Reading file from {input_path}")
    coastline_handler = CoastlineHandler(tags, mode)
    coastline_handler.apply_file(input_path, idx="flex_mem", locations=True)
    coastline_df = geopandas.GeoDataFrame(coastline_handler.coastlines, geometry="POLYGON_STR")
    coastline_df.to_file(f"{output_path}\\unmerged_coastline.geojson", driver="GeoJSON")

    # 2. Merge all coastline
    logging.info("[2/3] Merge all the coastline.")
    start_time = time.time()

    # Offline but cost more time
    # territorial_geom = get_limit_relation_geom(input_path, limit_relation_id)
    # Online, cost api loads
    territorial_geom = get_relation_polygon_with_overpy(limit_relation_id)
    tmp = read_file_and_rename_geometry(f"{output_path}\\unmerged_coastline.geojson")
    unmerged = prepare_data(tmp, territorial_geom.wkt, "POLYGON_STR")
    candidates = unmerged.set_index(unmerged["POLYGON_ID"]).to_dict('index')
    unmerged = deepcopy(candidates)
    unmerged_ids = list(unmerged.keys())
    i = len(unmerged_ids) - 1
    while i >= 0:
        try:
            unmerged_id = unmerged_ids[i]
            row = unmerged[unmerged_id]
            unmerged_ids.remove(unmerged_id)
            candidates.pop(unmerged_id)
            merge_with_candidates_dict(row, unmerged_ids, unmerged, candidates)
            i = len(unmerged_ids) - 1
            # candidates should be changed
        except:
            print("unmerged_id")
    merged = geopandas.GeoDataFrame.from_dict(unmerged, orient="index")
    merged = filter_small_island(merged, area_threshold=40000)
    merged.to_file("merged.geojson", driver="GeoJSON")

    logging.debug(f"Merging completed, taking {time.time() - start_time} seconds")
    # 3. re-merge and divide
    threshold = 100.0
    logging.info(f"[3/3] Extracting lengthy (length > 600km) geometry, re-merge and divide with threshold {threshold} km.")
    if DEBUGGING or divide:
        # Find all the coastline which length is larger than 600 km, divide it later.
        lengthy_geometry_ids = list(merged[lonlat_length_in_km(merged["POLYGON_STR"]) > 600]["POLYGON_ID"])
        merged_dict = dict()
        # Divide all the lengthy (LENGTH >600km) geometry
        logging.debug("Start re-merge and divide.")
        for lengthy_id in lengthy_geometry_ids:
            logging.debug(f"{lengthy_id} is being re-merged and divided.")
            lengthy_wkt = list(merged.loc[merged["POLYGON_ID"] == int(lengthy_id)]["POLYGON_STR"])[0].wkt
            tmp = read_file_and_rename_geometry(f"{output_path}\\unmerged.geojson")
            lengthy = prepare_data(tmp, lengthy_wkt, "POLYGON_STR")
            lengthy_dict = lengthy.set_index(lengthy["POLYGON_ID"]).to_dict('index')
            lengthy_merged_result = get_merged_and_divided_by_threshold(lengthy_dict, 60.0, 100.0)
            merged_dict[lengthy_id] = lengthy_merged_result
        # concat into merged
        for lengthy_id, lengthy_merged_result in merged_dict.items():
            lengthy_merged_result_df = geopandas.GeoDataFrame.from_dict(lengthy_merged_result, orient="index")
            merged = merged[merged["POLYGON_ID"] != lengthy_id]
            merged = pandas.concat([merged, lengthy_merged_result_df])
    else:
        logging.info(f"Detect divide mode OFF, no need to re-merging and dividing.")

    merged.set_geometry(col="POLYGON_STR", inplace=True)
    if DEBUGGING:
        merged.to_file(f"{output_path}\\coastlines.geojson", driver="GeoJSON")
    else:
        merged.to_csv("coastlines.tsv", sep="\t")
    # Optional
    # try:
    #     os.remove(f"{output_path}\\unmerged.geojson")
    #     os.remove(f"{output_path}\\merged.geojson")
    # except:
    #     traceback.print_exc()

    logging.debug("==================================")
    logging.debug("Merging and dividing coastline process completed")

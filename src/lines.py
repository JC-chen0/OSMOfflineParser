import logging.config
import logging
import time
import traceback
import os
import geopandas
import osmium
import pandas
from copy import deepcopy
from shapely import wkt
from src.enum.hofntype import HofnType
from src.util.limit_area import get_limit_relation_geom, get_relation_polygon_with_overpy
from util.merging_utils import lonlat_length_in_km, prepare_data, get_merged_and_divided_by_threshold, read_file_and_rename_geometry, merge_with_candidates, merge_with_candidates_dict, filter_small_island

# %%
wkt_factory = osmium.geom.WKTFactory()


class LineHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode):
        super().__init__()
        self.lines = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], "HOFN_TYPE": [], "ROAD_LEVEL": []}
        self.tags = tags
        self.mode = mode

    def way(self, w):
        line_id = w.id
        line_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        if any([w.tags.get(key) == value for key, value in self.tags.items()]):
            line = wkt.loads(wkt_factory.create_linestring(w))
            try:
                self.append_line_attribute(self.lines, line_id, line_name, line)
            except Exception as e:
                traceback.print_exc()

    def append_line_attribute(self, attributes: dict, line_id: str, name, geometry):
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        attributes["POLYGON_ID"].append(line_id)
        attributes["POLYGON_NAME"].append(name)
        attributes["POLYGON_STR"].append(geometry)
        attributes["HOFN_TYPE"].append(HofnType[self.mode].value)
        attributes["ROAD_LEVEL"].append(0)


def main(input_path, output_path, nation, limit_relation_id, divide, mode, tags, debugging=False):
    DEBUGGING = debugging

    # 1. Get coastlines data from osm.pbf file
    logging.info("============================================")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"PROCESSING NATION: {nation}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info(f"MODE: {mode}")
    logging.info(f"SEARCH TAG WITH VALUE: {tags}")
    logging.info(f"RE-MERGING AND DIVIDING MODE: {'ON' if divide else 'OFF'}")
    logging.info("============================================")

    logging.info("[1/3] Prepare line data from osm.pbf file.")
    logging.info(f"Reading file from {input_path}")
    line_handler = LineHandler(tags, mode)
    line_handler.apply_file(input_path, idx="flex_mem", locations=True)
    line_df = geopandas.GeoDataFrame(line_handler.lines, geometry="POLYGON_STR")
    line_df.to_file(f"{output_path}/unmerged.geojson", driver="GeoJSON")

    # 2. Merge all line
    logging.info("[2/3] Merge all the line.")
    start_time = time.time()

    # Offline but cost more time
    # territorial_geom = get_limit_relation_geom(input_path, limit_relation_id)
    # Online, cost api loads
    territorial_geom = get_relation_polygon_with_overpy(limit_relation_id)
    tmp = read_file_and_rename_geometry(f"{output_path}/unmerged.geojson")
    unmerged = prepare_data(tmp, territorial_geom.wkt, "POLYGON_STR")
    candidates = unmerged.set_index(unmerged["POLYGON_ID"]).to_dict('index')
    result = deepcopy(candidates)
    unmerged_ids = list(result.keys())
    i = len(unmerged_ids) - 1
    while i >= 0:
        unmerged_id = unmerged_ids[i]
        row = result[unmerged_id]
        unmerged_ids.remove(unmerged_id)
        candidates.pop(unmerged_id)
        merge_with_candidates_dict(row, unmerged_ids, result, candidates)
        i = len(unmerged_ids) - 1

    filtered = filter_small_island(result, area_threshold=40000)
    merged = geopandas.GeoDataFrame.from_dict(filtered, orient="index")
    merged = merged.set_geometry("POLYGON_STR")
    merged.to_file(f"{output_path}/merged.geojson", driver="GeoJSON")

    logging.debug(f"Merging completed, taking {time.time() - start_time} seconds")
    # 3. re-merge and divide
    threshold = 100.0
    logging.info(f"[3/3] Extracting lengthy (length > 600km) geometry, re-merge and divide with threshold {threshold} km.")
    if DEBUGGING or divide:
        # Find all the line which length is larger than 600 km, divide it later.
        lengthy_geometry_ids = list(merged[lonlat_length_in_km(merged["POLYGON_STR"]) > 600]["POLYGON_ID"])
        merged_dict = dict()
        # Divide all the lengthy (LENGTH >600km) geometry
        logging.debug("Start re-merge and divide.")
        for lengthy_id in lengthy_geometry_ids:
            logging.debug(f"{lengthy_id} is being re-merged and divided.")
            lengthy_wkt = list(merged.loc[merged["POLYGON_ID"] == int(lengthy_id)]["POLYGON_STR"])[0].wkt
            tmp = read_file_and_rename_geometry(f"{output_path}/unmerged.geojson")
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
        merged.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON")
    else:
        merged.to_csv(f"{output_path}/{mode}.tsv", sep="\t")

    if DEBUGGING is not True:
        try:
            os.remove(f"{output_path}/unmerged.geojson")
            os.remove(f"{output_path}/merged.geojson")
        except:
            traceback.print_exc()

    logging.debug("==================================")
    logging.debug("Merging and dividing line process completed")

import logging.config
import logging
import math
import time
import traceback
import os
import geopandas
import osmium
import pandas
from copy import deepcopy
from shapely import wkt, ops
from src.enum.hofn_type import HofnType
from src.util.limit_area import get_relation_polygon_with_overpy, prepare_data, get_limit_relation_geom
from src.util.merging_utils import merge_with_candidates_dict, filter_small_island, lonlat_length_in_km, get_merged_and_divided_by_threshold, is_reverse_needed, reverse_linestring_coords, is_continuous, linemerge_by_wkt
from src.util.read_data import read_file_and_rename_geometry
from src.enum.tag import Tag

# %%
wkt_factory = osmium.geom.WKTFactory()


class LineHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode, level=None):
        super().__init__()
        self.lines = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], "HOFN_TYPE": [], "ROAD_LEVEL": []}
        self.tags = tags
        self.mode = mode
        self.level = level

    def way(self, w):
        line_id = w.id
        line_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        if any([w.tags.get(key) in value if type(value) == list else w.tags.get(key) == value for key, value in self.tags.items()]):
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
        attributes["POLYGON_STR"].append(geometry)
        attributes["HOFN_TYPE"].append(HofnType[self.mode].value)
        attributes["ROAD_LEVEL"].append(level)


def main(input_path, output_path, nation, limit_relation_id, mode, tags, DEBUGGING=False, DIVIDE=None, LEVEL_DICT=None):
    IS_LEVEL = True if LEVEL_DICT else False
    IS_RING = True if mode in ["coastline"] else False
    IS_FERRY = True if mode in ["ferry"] else False
    ###############################################################################################
    # 1. GET DATA
    # logging.info("[1/3] Prepare line data from osm.pbf file.")
    # logging.info(f"Reading file from {input_path}")
    # line_handler = LineHandler(tags, mode, LEVEL_DICT)
    # line_handler.apply_file(input_path, idx="flex_mem", locations=True)
    # line_df = geopandas.GeoDataFrame(line_handler.lines, geometry="POLYGON_STR")
    # line_df.to_file(f"{output_path}/unmerged.geojson", driver="GeoJSON", encoding="utf-8")
    # ###############################################################################################
    # # 2. MERGE ALL LINE
    # logging.info("[2/3] Merge all the line.")
    # start_time = time.time()
    #
    # # Offline but cost more time
    # territorial_geom = get_limit_relation_geom(input_path, limit_relation_id)
    # # Online, cost api loads
    # # territorial_geom = get_relation_polygon_with_overpy(limit_relation_id)
    # tmp = read_file_and_rename_geometry(f"{output_path}/unmerged.geojson")
    # data = prepare_data(tmp, territorial_geom.wkt, "POLYGON_STR")
    data = read_file_and_rename_geometry(f"{output_path}/unmerged.geojson")
    levels = Tag.get_levels(mode, LEVEL_DICT) if IS_LEVEL else [0]
    unmergeds = [data[data["ROAD_LEVEL"] == level] for level in levels]
    result = {"POLYGON_ID": [], "POLYGON_NAME": [], "POLYGON_STR": [], "HOFN_TYPE": [], "ROAD_LEVEL": []}
    for unmerged in unmergeds:
        logging.info(f"Start process level = {unmerged.iloc[0]['ROAD_LEVEL']}")
        unmerged_in_current_level: dict = unmerged.set_index(unmerged["POLYGON_ID"]).to_dict('index')
        unmerged_values = list(unmerged_in_current_level.values())

        i = len(unmerged_values) - 1
        j = i - 1
        while len(unmerged_values):
            try:
                mainline = unmerged_values[i]["POLYGON_STR"]
                candidate = unmerged_values[j]["POLYGON_STR"]
                if is_reverse_needed(mainline, candidate):
                    logging.debug(f"{unmerged_values[j]['POLYGON_ID']} reversed.")
                    candidate = reverse_linestring_coords(candidate)
                if is_continuous(mainline, candidate):
                    logging.debug(f"{unmerged_values[i]['POLYGON_ID']} merged with {unmerged_values[j]['POLYGON_ID']}.")
                    mainline = linemerge_by_wkt(mainline, candidate)
                    unmerged_values[i]["POLYGON_STR"] = mainline
                    unmerged_values.pop(j)
                    i = len(unmerged_values) - 1
                    j = i - 1
                else:
                    j -= 1
                if i == 0:
                    print("")

                if j == 0 or i == 0:
                    result.get("POLYGON_ID").append(unmerged_values[i]["POLYGON_ID"])
                    result.get("POLYGON_NAME").append(unmerged_values[i]["POLYGON_NAME"])
                    result.get("POLYGON_STR").append(unmerged_values[i]["POLYGON_STR"])
                    result.get("HOFN_TYPE").append(unmerged_values[i]["HOFN_TYPE"])
                    result.get("ROAD_LEVEL").append(unmerged_values[i]["ROAD_LEVEL"])
                    unmerged_values.pop(i)
                    i = len(unmerged_values) - 1
                    j = i - 1
            except:
                print("")
        # candidates = unmerged.set_index(unmerged["POLYGON_ID"]).to_dict('index')
        # level_result = deepcopy(candidates)
        # unmerged_ids = list(level_result.keys())
        # i = len(unmerged_ids) - 1
        # while i >= 0:
        #     unmerged_id = unmerged_ids[i]
        #     row = level_result[unmerged_id]
        #     unmerged_ids.remove(unmerged_id)
        #     candidates.pop(unmerged_id)
        #     merge_with_candidates_dict(row, unmerged_ids, level_result, candidates)
        #     i = len(unmerged_ids) - 1
        # result.update(level_result)
    #############################################################################
    # After merging, we need some operations with difference mode
    # ONLY those linestring being ringed need to filter with area threshold
    if IS_RING:
        result = filter_small_island(result, area_threshold=40000)
    merged = geopandas.GeoDataFrame.from_dict(result, orient="index")
    if IS_FERRY:
        merged["POLYGON_STR"] = merged.POLYGON_STR.apply(lambda polygon_str: polygon_str.buffer(15 / 6371000 / math.pi * 180))

    merged = merged.set_geometry("POLYGON_STR")
    merged.to_file(f"{output_path}/merged.geojson", driver="GeoJSON")
    logging.debug(f"Merging completed, taking {time.time() - start_time} seconds")
    ###########################################################################################
    # [OPTIONAL] 3. re-merge and DIVIDE
    threshold = 100.0
    logging.info(f"[OPTIONAL][3/3] Extracting geometry, re-merge and DIVIDE with threshold {threshold} km.")
    if DIVIDE:
        merged = geopandas.read_file(f"{output_path}/merged.geojson", driver="GeoJSON")
        divide_result_dict = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'ROAD_LEVEL': []}
        # Find all the line which length is larger than [user-set] km, DIVIDE it later.
        lengthy_geometry_ids = DIVIDE
        merged_dict = dict()
        # Divide all the lengthy (LENGTH >600km) geometry
        logging.debug("Start re-merge and DIVIDE.")
        for lengthy_id in lengthy_geometry_ids:
            logging.debug(f"{lengthy_id} is being re-merged and divided.")
            lengthy_wkt = list(merged.loc[merged["POLYGON_ID"] == int(lengthy_id)]["POLYGON_STR"])[0].wkt
            tmp = read_file_and_rename_geometry(f"{output_path}/unmerged.geojson")
            lengthy = prepare_data(tmp, lengthy_wkt, "POLYGON_STR")
            lengthy_dict = lengthy.set_index(lengthy["POLYGON_ID"]).to_dict('index')
            lengthy_merged_result = get_merged_and_divided_by_threshold(lengthy_dict, divide_result_dict, 60.0, 100.0)
            merged_dict[lengthy_id] = lengthy_merged_result
        # concat into merged
        for lengthy_id, lengthy_merged_result in merged_dict.items():
            # lengthy_merged_result_df = geopandas.GeoDataFrame.from_dict(lengthy_merged_result, orient="index")
            lengthy_merged_result_df = geopandas.GeoDataFrame(lengthy_merged_result, geometry="POLYGON_STR")
            merged = merged[merged["POLYGON_ID"] != lengthy_id]
            merged = pandas.concat([lengthy_merged_result_df, merged])
    else:
        logging.info(f"Detect DIVIDE mode OFF, no need to re-merging and dividing.")

    #####################################################################################
    # OUTPUT
    merged.set_geometry(col="POLYGON_STR", inplace=True)
    if DEBUGGING:
        merged.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")
    else:
        merged.to_csv(f"{output_path}/{mode}.tsv", sep="\t", index=False)
        merged.to_file(f"{output_path}/{mode}.geojson", driver="GeoJSON", encoding="utf-8")

    logging.debug("==================================")
    logging.debug("Merging and dividing line process completed")

import logging.config
import logging
import math
import time
import traceback
from argparse import ArgumentParser
from datetime import date
from typing import Dict

import geopandas
import osmium
import pandas
import yaml
from geopandas import GeoDataFrame
from shapely import wkt
from shapely.ops import polygonize
from utils.merging_utils import get_relation_polygon, lonlat_length_in_km, prepare_data, get_merged, \
    get_merged_and_divided_by_threshold, read_file_and_rename_geometry

# %%
wkt_factory = osmium.geom.WKTFactory()


class CoastlineHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.coastlines = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], "HOFN_TYPE": [], "HOFN_LEVEL": []}

    def way(self, w):
        self.get_coastlines(w)

    def get_coastlines(self, w):
        coastline_id = w.id
        coastline_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        natural = w.tags.get("natural")
        if natural == "coastline":
            coastline = wkt.loads(wkt_factory.create_linestring(w))
            try:
                self.append_coastline_attribute(self.coastlines, coastline_id, coastline_name, coastline)
            except Exception as e:
                pass

    def append_coastline_attribute(self, attributes: dict, coastline_id: str, name, geometry):
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        attributes["POLYGON_ID"].append(coastline_id)
        attributes["POLYGON_NAME"].append(name)
        attributes["POLYGON_STR"].append(geometry)
        attributes["HOFN_TYPE"].append(2)
        attributes["HOFN_LEVEL"].append(1)


def filter_small_island(data: dict, area_threshold: int):
    start_time = time.time()
    #  filter the small island, where there is no people
    del_ids = []
    for id, coastline in data.items():
        # will only have a polygon
        if list(polygonize(coastline.get("geometry")))[0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
            del_ids.append(id)

    [data.pop(del_id) for del_id in del_ids]
    logging.debug("=================================")
    logging.debug(f"Area filter process completed, taking: {time.time() - start_time} seconds")


# %%
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("output", type=str, help="Output geojson file path.")
    parser.add_argument("limit_relation", type=str, help="Relation id of limit area.")
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    limit_relation_id = args.limit_relation

    try:
        with open('src/resource/logback.yaml', 'r') as stream:
            config = yaml.safe_load(stream)
            config.get("handlers").get("info_file_handler")["filename"] = f"logs\\coastline\\{limit_relation_id}-{date.today()}.info"
            config.get("handlers").get("debug_file_handler")["filename"] = f"logs\\coastline\\{limit_relation_id}-{date.today()}.debug"
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
    logging.info("============================================")

    coastline_handler = CoastlineHandler()
    coastline_handler.apply_file(input_path, idx="flex_mem", locations=True)
    coastline_df = geopandas.GeoDataFrame(coastline_handler.coastlines, geometry="POLYGON_STR")
    coastline_df.to_file(f"{output_path}\\unmerged_coastline.geojson", driver="GeoJSON")

    #  Merge all coastline
    taiwan_territorial_geom = get_relation_polygon(limit_relation_id)
    tmp = read_file_and_rename_geometry(f"{output_path}\\unmerged_coastline.geojson")
    coastline = prepare_data(tmp, taiwan_territorial_geom.wkt, "POLYGON_STR")
    coastline_merge_dict = get_merged(coastline)
    filter_small_island(coastline_merge_dict, area_threshold=40000)
    df = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
    df.to_file(f"{output_path}\\merged_coastline.geojson", driver="GeoJSON")
    logging.debug("=================================")
    logging.debug("Merging coastline process completed")

    #  Merging taiwan mainland
    merged_coastline: GeoDataFrame = read_file_and_rename_geometry(f"{output_path}\\merged_coastline.geojson")
    # Find all the coastline which length is larger than 400 km, divide it later.
    lengthy_geometry_ids = list(merged_coastline[lonlat_length_in_km(merged_coastline["GEOMETRY"]) > 400]["POLYGON_ID"])
    merged_dict: Dict[Dict] = dict()
    # Divide all the lengthy (LENGTH >400km) geometry
    for lengthy_id in lengthy_geometry_ids:
        lengthy_wkt = list(merged_coastline.loc[merged_coastline["POLYGON_ID"] == int(lengthy_id)]["POLYGON_STR"])[0].wkt
        tmp = read_file_and_rename_geometry(f"{output_path}\\unmerged_coastline.geojson")
        lengthy = prepare_data(tmp, lengthy_wkt, "POLYGON_STR")
        lengthy_dict = lengthy.set_index(lengthy["POLYGON_ID"]).to_dict('index')
        lengthy_merged_result = get_merged_and_divided_by_threshold(lengthy_dict, 60.0, 100.0)
        merged_dict[lengthy_id] = lengthy_merged_result
    # concat into merged_coastline
    for lengthy_id, lengthy_merged_result in merged_dict:
        lengthy_merged_result_df: geopandas.GeoDataFrame = geopandas.GeoDataFrame.from_dict(lengthy_merged_result, orient="index")
        merged_coastline = merged_coastline[merged_coastline["POLYGON_ID"] != lengthy_id]
        merged_coastline = pandas.concat([merged_coastline, lengthy_merged_result_df])

    logging.debug("==================================")
    logging.debug("Merging and dividing coastline process completed")

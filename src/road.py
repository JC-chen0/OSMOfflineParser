import logging
import os
import time
import traceback
from argparse import ArgumentParser
import osmium
import yaml
from shapely import wkt
import geopandas
from shapely.geometry import LineString
from src.util.merging_utils import get_merged

# %%
wkt_factory = osmium.geom.WKTFactory()


class RoadHandler(osmium.SimpleHandler):
    roads_type = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    roads_with_level = dict(zip(roads_type, [1, 2, 3, 4, 5]))

    def __init__(self):
        super().__init__()
        self.roads = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], 'HOFN_TYPE': [], 'HOFN_LEVEL': []}

    def way(self, w):
        self.get_ways(w)

    def get_ways(self, w):
        way_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        road = w.tags.get("road")
        if road in RoadHandler.roads_type:
            line = wkt.loads(wkt_factory.create_linestring(w))
            self.append_way_attribute(way_id, name, line, self.get_way_level(road))

    def get_way_level(self, road_tag: str) -> int:
        return RoadHandler.roads_with_level.get(road_tag)

    def append_way_attribute(self, way_id: str, name: str, line: LineString, level: int):
        self.roads.get('POLYGON_ID').append(way_id)
        self.roads.get('POLYGON_NAME').append(name)
        self.roads.get('POLYGON_STR').append(line)
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        self.roads.get('HOFN_TYPE').append(7)
        self.roads.get('HOFN_LEVEL').append(level)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("output", type=str, help="Output geojson file path.")
    parser.add_argument("limit_relation", type=str, help="Relation id of limit area.")
    parser.add_argument("--mode",type=str, help="Process mode:[road], Output file name")
    parser.add_argument("--tags", type=str, help="format: tag_name1 search_value1 tag_name2 search_value2", nargs="+")

    arg = parser.parse_args()
    input_path = arg.input
    output_path = arg.output
    limit_relation_id = arg.limit_relation
    mode = arg.mode
    tags = {}
    tmp = 0
    while tmp < len(arg.tags) - 1:
        tag = arg.tags[tmp]
        value = arg.tags[tmp + 1]
        tags[arg.tags[tmp]] = arg.tags[tmp + 1]
        tmp += 2

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
    logging.info(f"WORKING DIRECTORY: {os.getcwd()}")
    logging.info(f"INPUT ARGUMENTS: {arg}")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info(f"MODE: {mode}")
    logging.info(f"SEARCH TAG WITH VALUE: {tags}")
    logging.info("============================================")

    logging.info(f"Start extracting waters ...")
    logging.info(f"[1/4] Loading data from {input_path}, tags: {tags}")

    handler = RoadHandler()
    handler.apply_file(input_path, locations=True, idx="flex_mem")
    result = geopandas.GeoDataFrame(handler.roads, geometry="POLYGON_STR")
    result.to_file(f"{output_path}unmerged_road.geojson", driver="GeoJSON")

    unmerged_roads = geopandas.read_file(f"{output_path}unmerged_road.geojson")
    merged_start_time = time.time()
    merged_road_dict = get_merged(unmerged_roads)
    merged_end_time = time.time()
    print(f"Merged road process completed, taking {merged_end_time - merged_start_time}")
    result = geopandas.GeoDataFrame.from_dict(merged_road_dict, orient="index")
    result.to_file("data\\output\\road\\merged_road.geojson", driver="GeoJSON")


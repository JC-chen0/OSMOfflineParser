
import logging.config
import logging
import math
import multiprocessing
import sys
import traceback
import os
import geopandas
import osmium
import pandas
from shapely import wkt
from src.utils import LimitAreaUtils, LineUtils
from src.enum import Tag, HofnType
from src.models import HofnData, RelationMember
wkt_factory = osmium.geom.WKTFactory()
cpu_count = multiprocessing.cpu_count() - 1 if multiprocessing.cpu_count() < 20 else 20



class LineHandler(osmium.SimpleHandler):
    def __init__(self, tags, mode, level=None):
        super().__init__()
        self.lines = []
        self.relations = dict()
        self.tags = tags
        self.mode = mode
        self.level = level

     # Tags: 1. Value 2. list 3. "" (purely take all the tags)
    def relation(self, relation):
        if any([relation.tags.get(key) in value if type(value) == list else relation.tags.get(key) == value if value != "" else relation.tags.get("key") for key, value in self.tags.items()]):
            for member in relation.members:
                if member.type == "w":
                    if not self.relations.get(relation.id, False):
                        self.relations[relation.id] = []
                    self.relations[relation.id].append(RelationMember(member.ref, member.type, member.role if member.role != "" else "outer"))
        
    # Tags: 1. Value 2. list 3. "" (purely take all the tags)
    def way(self, w):
        line_id = w.id
        line_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        if any([w.tags.get(key) in value if type(value) == list else w.tags.get(key) == value if value != "" else w.tags.get("key") for key, value in self.tags.items()]):
            line = wkt.loads(wkt_factory.create_linestring(w))
            level = self.level.get(w.tags.get(self.mode), False) if self.level else 0  # For LEVEL_DICT-need way
            if level is not False:
                try:
                    self.lines.append(HofnData(line_id, line_name, HofnType[self.mode].value, level, line))
                except Exception as e:
                    traceback.print_exc()



def main(input_path, output_path, nation, limit_relation_id, mode, tags, DEBUGGING=False, DIVIDE=None, LEVEL_DICT=None, ALL_OFFLINE=True):
    IS_LEVEL = True if LEVEL_DICT else False
    IS_RING = True if mode in ["coastline"] else False
    IS_FERRY = True if mode in ["ferry"] else False
    levels = Tag.get_levels(mode, LEVEL_DICT) if IS_LEVEL else [0]
    ###############################################################################################
    # 1. GET DATA
    if not DIVIDE:
        logging.info("[1/2] Prepare line data from osm.pbf file.")
        logging.info(f"Reading file from {input_path}")
        # 1.1. Read osm.pbf file
        line_handler = LineHandler(tags, mode, LEVEL_DICT)
        line_handler.apply_file(input_path, idx="flex_mem", locations=True)
        lines = line_handler.lines
        relation_member_dict = line_handler.relations
        del line_handler
        ################################################################################################
        
        lines_df = geopandas.GeoDataFrame([vars(i) for i in lines])
        lines_dict = lines_df.set_index("POLYGON_ID", drop=False).to_dict("index")
        
        logging.info("Getting data from relations.")
        # 1.2. Get limit area
        logging.info("Loading limit area geometry.")
        if ALL_OFFLINE:
            logging.info("Detect all offline mode on, using offline file to load limit area")
            limit_area = LimitAreaUtils.get_limit_relation_geom(input_path, limit_relation_id)
        else:
            logging.info("Detect all offline mode off, using api to load limit area")
            limit_area = LimitAreaUtils.get_relation_polygon_with_overpy(limit_relation_id)
        
        
        ###############################################################################################
        # 2. MERGE ALL LINE
        logging.info("[2/2] Merge all the line.")
        
        # Highway mode
        if mode == "highway":
            logging.info("Merging way in same relation.")
            # 2.1.2 Get relation data
            relations = dict()
            # ONLY search for those match the tags
            for relation_id, relation_members in relation_member_dict.items():
                hofn_datas = list(map(LineUtils.get_relation_data, relation_members, [lines_dict] * len(relation_members)))
                hofn_datas = [i for i in hofn_datas if i is not None]
                if hofn_datas: # If there is no data in the relation, it will be ignored.
                    relations[relation_id] = hofn_datas
            
            relations =  {relation_id:geopandas.GeoDataFrame([vars(i) for i in relation_members]) for relation_id, relation_members in relations.items()} # Convert to GeoDataFrame for intersects use.
            id_used_list = list()
            relations_result = [LineUtils.get_merged_members(relation_members,levels,id_used_list) for relation_id,relation_members in relations.items()]
            relations_result = sum(relations_result, []) # flatten the list from level 1 to 5
            geopandas.GeoDataFrame(relations_result).to_file("relations_result.geojson", driver="GeoJSON")

            # 2.2. concat relation result to lines from ways, and do one more time intersects merge.    
            logging.info("Merging remaining lines from ways.")
            data_from_way = LimitAreaUtils.prepare_data(lines_df, limit_area.wkt)
            data_from_way = data_from_way[~data_from_way["POLYGON_ID"].isin(id_used_list)]
            data = pandas.concat([data_from_way, geopandas.GeoDataFrame(relations_result)], ignore_index=True)
            unmerged_way_split_by_level = [data[data["ROAD_LEVEL"] == level] for level in levels]
            result = [LineUtils.merge_by_intersects(i) for i in unmerged_way_split_by_level if not i.empty]
            result = sum(result, []) # flatten list from level1 to level5
            
        # other mode
        else:
            data_from_way = LimitAreaUtils.prepare_data(lines_df, limit_area.wkt)
            data = data_from_way
            unmerged_way_split_by_level = [data[data["ROAD_LEVEL"] == level] for level in levels]
            result = [LineUtils.merge_by_intersects(i) for i in unmerged_way_split_by_level if not i.empty]
            result = sum(result, []) # flatten list from level1 to level5
        
        logging.info("Merge completed.")
        #############################################################################
        # After merging, we need some operations with difference mode
        # ONLY those linestring being ringed need to filter with area threshold
        if IS_RING:
            result = LineUtils.filter_small_island(result, area_threshold=40000)
        merged = geopandas.GeoDataFrame(result)
        if IS_FERRY:
            merged["geometry"] = merged.geometry.apply(lambda geometry: geometry.buffer(15 / 6371000 / math.pi * 180))
        merged.to_file(f"{output_path}/merged.geojson", driver="GeoJSON", index=False, encoding="utf-8")


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

# %%

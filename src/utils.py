import logging
import math
import multiprocessing
import time
from itertools import islice
from typing import Dict, List
import geopandas
import numpy
import overpy
import pandas
import shapely.ops
import osmium
from shapely.geometry import LineString, Polygon, Point, MultiPolygon
from shapely.ops import linemerge, unary_union, polygonize
from shapely import wkt
from src.models import HofnData
from src.enum import HofnType


def reverse_linestring_coords(geometry):

    reverse = geometry
    reverse.coords = list(reverse.coords)[::-1]
    return reverse


def is_continuous(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_tail or tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_head or tail == compare_tail


def lonlat_length_in_km(geom):
    return geom.length * 6371 * math.pi / 180


def linemerge_by_wkt(line1, line2) -> LineString:
    line1_coords = line1.coords[:]
    line2_coords = line2.coords[:]
    source, target = (line1, line2) if line1_coords[-1] == line2_coords[0] else (line2, line1)
    coords = source.coords[:]
    coords.pop(-1)
    coords.extend(target.coords[:])
    new_linestring = LineString(coords)
    return new_linestring

def brute_force_merge(line1,line2,from_tail=True) -> LineString:
    line1_coords = line1.coords[:]
    line2_coords = line2.coords[:]
    line1_coords.pop(-1) if from_tail else line2_coords.pop(-1)
    line1_coords.extend(line2_coords) if from_tail else line2_coords.extend(line1_coords)
    new_linestring = LineString(line1_coords)
    return new_linestring

class LineUtils:

    def merge_by_intersects(unmerged_level_roads:geopandas.GeoDataFrame,id_used_list=False):    
        unmerged_way = unmerged_level_roads.copy(deep=True) # copy to avoid changing original data, original data will used to check what id is used
        unmerged_way = unmerged_way.reset_index(drop=True) # reset index for loc issue, sindex intersects will check for labeled index. 
        result = [] # Generate geodataframe result
        
        # Prepare init iteration
        processing, unmerged_way = unmerged_way.iloc[-1], unmerged_way.iloc[:-1] # pop row to processing
        index_used_list = [processing.name] # init index used list with init line
        if unmerged_way.empty: # if unmerged_way is empty, return processing
            result.append(processing)
            return result


        while len(unmerged_way) > 0:
            
            got_merged = False # Flag to check if line is merged

            possible_matches_index = unmerged_way.sindex.query(processing.geometry, predicate='intersects') # Get a list of labeled index that intersect with processing line
            precise_matches_index = [i for i in possible_matches_index if i not in index_used_list]
            precise_matches = unmerged_way.loc[precise_matches_index] # Using labeled index to find precise matches
            precise_matches = precise_matches.to_dict("index") # convert into dict to apply performance.
            
            # Merge the lines that intersect with the processing line
            for row_index,row in precise_matches.items():
                row_geometry = row["geometry"]
                if is_reverse_needed(processing.geometry, row_geometry):
                    row_geometry = reverse_linestring_coords(row_geometry)
                if is_continuous(processing.geometry, row_geometry):
                    processing.geometry = linemerge_by_wkt(processing.geometry, row_geometry)

                    index_used_list.append(row_index)
                    got_merged = True

            if got_merged: 
                continue  # If the processing line is merged, continue to the next loop
            
            # If the processing line is not merged, add it to the result
            result.append(processing)   
            unmerged_way = unmerged_way[~unmerged_way.index.isin(index_used_list)]
            
            # If there are lines to process, continue with last element
            if not unmerged_way.empty:
                processing, unmerged_way = unmerged_way.iloc[-1], unmerged_way.iloc[:-1] # pop last row to processing
                index_used_list.append(processing.name)

        id_used_list += list(unmerged_level_roads[~unmerged_level_roads.index.isin(unmerged_way.index)]["POLYGON_ID"].values) # Add the id of the used line to the id_used_list
        return result
            
    @staticmethod
    def get_merged_and_divided(geometry_dict, origin_line, length_threshold):
        unmerged_values = list(geometry_dict.values())
        start_geometry = NotImplemented
        # Find start id and geometry
        for i in unmerged_values:
            # TODO: Check if generate from offline also need buffer or not
            if i["geometry"].intersects(Point(origin_line.coords[0]).buffer(0.00001)):
                start_geometry = i
                break
        unmerged_values.remove(start_geometry)
        unmerged_values.append(start_geometry)

        i = len(unmerged_values) - 1
        j = i - 1
        merged_result = []
        while len(unmerged_values) > 0:
            mainline = unmerged_values[i]["geometry"]
            candidate = unmerged_values[j]["geometry"]
            if is_reverse_needed(mainline, candidate):
                candidate = reverse_linestring_coords(candidate)
            if is_continuous(mainline, candidate):
                logging.debug(f"{unmerged_values[i]['POLYGON_ID']} merged with {unmerged_values[j]['POLYGON_ID']}.")
                if linemerge_by_wkt(mainline, candidate).length * 6371 * math.pi / 180 > length_threshold and mainline.length * 6371 * math.pi / 180 > 80:
                    logging.debug(f"{unmerged_values[i]['POLYGON_ID']} meet length threshold, using {unmerged_values[j]['POLYGON_ID']} start.")
                    # append meet threshold geometry and pop.
                    merged_result.append(unmerged_values[i])
                    unmerged_values.pop(i)
                    # Using next candidate start.
                    tmp = unmerged_values[j]
                    unmerged_values.remove(tmp)
                    unmerged_values.append(tmp)
                else:
                    mainline = linemerge_by_wkt(mainline, candidate)
                    unmerged_values[i]["geometry"] = mainline
                    unmerged_values.pop(j)
                # reset
                i = len(unmerged_values) - 1
                j = i - 1
            else:
                j = j - 1

            # no more line can be linked.
            if j < 0:
                merged_result.append(unmerged_values[i])
                unmerged_values.pop(i)
                i = len(unmerged_values) - 1
                j = i - 1
                if i > 0:
                    logging.debug(f"{unmerged_values[i]['POLYGON_ID']} start merge.")
                # Stop merging process and append last segment.
                if i == 0:
                    merged_result.append(unmerged_values[i])
                    unmerged_values.pop(i)
        return merged_result


    @staticmethod
    def filter_small_island(merged, area_threshold: int):
        #  filter the small island, where there is no people
        filtered = merged
        small_island_list = []
        index = 0
        for values in merged:
            key = values["POLYGON_ID"]
            geometry = values["geometry"]
            try:
                if list(polygonize(geometry))[0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
                    small_island_list.append(index)
            except:
                logging.debug(f"POLYGON_ID: {key} cannot be polygonized.")
            finally:
                index += 1

        for index in sorted(small_island_list, reverse=True):
            filtered.pop(index)
        return filtered

    @staticmethod
    def get_way_geometry_from_overpy(way_id):
        api = overpy.Overpass()
        query_message = f"""[out:json][timeout:25]
            way({way_id}); 
                        out body;
                        >;
                        out skel qt;
                        """
        lineStrings = []
        result = api.query(query_message)
        for key, way in enumerate(result.ways):
            linestring_coords = []
            for node in way.nodes:
                linestring_coords.append(Point(node.lon, node.lat))
            lineStrings.append(LineString(linestring_coords))

        return lineStrings

    @staticmethod
    def polygonize_with_try_catch(row, remove_list):
        try:
            return list(shapely.ops.polygonize(row["geometry"]))[0]
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['geometry']}, return origin LINESTRING instead")
            remove_list.append(row['POLYGON_ID'])
            return row["geometry"]

    @staticmethod
    def get_relation_data(relation_member, lines_dict):
        way_from_relations = lines_dict.get(relation_member.id,False)
        if way_from_relations:
            return HofnData(way_from_relations["POLYGON_ID"], way_from_relations["POLYGON_NAME"], way_from_relations["HOFN_TYPE"], way_from_relations["ROAD_LEVEL"], way_from_relations["geometry"])

    @staticmethod
    def get_merged_members(relation_members_df,  levels, id_used_list=[]):
        original_id_used_list = id_used_list[:]
        current_id_used_list = original_id_used_list[:]
        # Get all the line split by level, remove empty levels.
        relation_members_split_by_level = [relation_members_df[relation_members_df["ROAD_LEVEL"] == level] for level in levels if not relation_members_df[relation_members_df["ROAD_LEVEL"] == level].empty]
        # Check unmerged line in each level
        unmerged_relation_members_split_by_level = [relation_members_df[~relation_members_df.POLYGON_ID.isin(current_id_used_list)] for relation_members_df in relation_members_split_by_level] 
        # Merge those unmerged in each level, remove those empty levels which remain no ununused line.   
        merged_relation_members = [LineUtils.merge_by_intersects(i, current_id_used_list) for i in unmerged_relation_members_split_by_level if not i.empty] # Merge by intersects
        id_used_list += list(set(current_id_used_list)-set(original_id_used_list))
        return sum(merged_relation_members, []) # flatten list

#################################################
class RingUtils:
    @staticmethod
    def get_relation_member_data(relation_dict: Dict, way_dict: Dict, tags: list) -> list:
        result = []
        for relation_id, members in relation_dict.items():
            valid_members = [member for member in members if member.role in tags and way_dict.get(member.id, False)]
            for member in valid_members:          
                way = way_dict[member.id]
                relation_member = {"relation_id": relation_id, "way_id": way.id, "name": way.name, "geometry": way.geometry, "role": member.role, type: member.type}
                result.append(relation_member)
        return result


    @staticmethod
    def restructure(relation_member_dict):
        temp = dict()
        for member in relation_member_dict.values():
            relation_id = member.get("relation_id")

            member.pop("relation_id")
            if not temp.get(relation_id, 0):
                temp[relation_id] = {"outer": [], "inner": []}

            if member.get("role") == "inner":
                temp.get(relation_id).get("inner").append(member)
            elif member.get("role") == "outer":
                temp.get(relation_id).get("outer").append(member)
            else:  # ONLY for debug purpose.
                logging.debug(f"Find way {member.get('way_id')} with invalid role {member.get('role')}.")
        return temp

    @staticmethod
    def islands_extracting(inners: List[Dict], islands: List[Dict]):
        for inner in inners:
            append = {"POLYGON_ID": inner.get("POLYGON_ID"),
                      "POLYGON_NAME": inner.get("POLYGON_NAME"),
                      "geometry": inner.get("geometry"),
                      "HOFN_TYPE": "5",
                      "ROAD_LEVEL": "0"}
            islands.append(append)

    # TODO: Optimize
    @staticmethod
    def get_merged_rings(rings: list, polygon_id_used_table: list, mode) -> List[Dict]:
        def get_merged_line(ring, merging_candidates: list, current_merged_ids) -> LineString:
            # Avoid merge with self
            current_merged_ids.append(ring["polygon_id"])

            merging_line = ring.get("geometry")

            candidate_line = NotImplemented
            merging_index = 0
            while merging_index < len(merging_candidates):
                candidate = merging_candidates[merging_index]
                candidate_line = candidate.get("geometry")
                candidate_id = candidate.get("polygon_id")
                try:
                    if candidate.get('polygon_id') in current_merged_ids:
                        merging_index += 1
                    else:
                        if is_reverse_needed(merging_line, candidate_line):
                            # Reverse the line and do merge with current index again.
                            logging.debug(f"candidate {candidate_id} reversed.")
                            candidate_line = reverse_linestring_coords(candidate_line)
                        if is_continuous(merging_line, candidate_line):
                            logging.debug(f"{ring.get('polygon_id')} merge with {candidate_id}")
                            # merge and start new round of iteration.
                            merging_line = linemerge_by_wkt(merging_line, candidate_line)
                            current_merged_ids.append(candidate_id)
                            merging_index = 0
                        else:
                            merging_index += 1
                except:
                    print(f"{candidate.get('way_id')} has some problems.")
            logging.debug(f"Return {merging_line}")
            return merging_line

        #################################################################################
        # Deep copy with merge candidate.
        merging_candidate = [ring for ring in rings]
        result = []
        current_merged_ids = []
        for ring in rings:
            # If being merged, skip it
            if ring.get('polygon_id') in current_merged_ids:
                continue

            logging.debug(f"WAY:{ring.get('polygon_id')} start doing merge.")
            merged_line = get_merged_line(ring, merging_candidate, current_merged_ids)

            # Choose way_id from merged line.
            for merged_id in current_merged_ids:
                if merged_id not in polygon_id_used_table:
                    logging.debug(f"{merged_id} is choosed as polygon id.")
                    result.append({'POLYGON_ID': merged_id, "POLYGON_NAME": ring.get("name"), "geometry": merged_line, "HOFN_TYPE": HofnType[mode].value, "ROAD_LEVEL": 0})
                    polygon_id_used_table.append(merged_id)
                    break

        return result

    @staticmethod
    def polygonize_with_try_catch(row, remove_list):
        try:
            return list(shapely.ops.polygonize(row["geometry"]))[0]
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['geometry']}, return origin LINESTRING instead")
            remove_list.append(row['POLYGON_ID'])
            return row["geometry"]

    @staticmethod
    def get_rings_merged_results(relation_member_dict, relation_result, islands, polygon_id_used_table, mode) -> tuple:
        # Outer then inner
        for relation_id, relation in relation_member_dict.items():
            logging.debug(f"Relation: {relation_id} doing merge.")

            outers = relation.get("outer")
            if outers:
                outers = RingUtils.get_merged_rings(outers, polygon_id_used_table, mode)
                relation_member_dict[relation_id] = outers
                for outer in outers:
                    relation_result.append(outer)
            

            # Remove inner as islands.
            if mode == "water":
                inners = relation.get("inner")
                if inners:
                    inners = RingUtils.get_merged_rings(inners, polygon_id_used_table, "island")
                    RingUtils.islands_extracting(inners, islands)

class MPUtils:
    # Dict divided to subdict
    @staticmethod
    def chunks(data, size):
        it = iter(data)
        for i in range(0, len(data), size):
            yield {k: data[k] for k in islice(it, size)}

    # https://stackoverflow.com/questions/24483182/python-split-list-into-n-chunks
    @staticmethod
    def chunks_set(l, n):
        """Yield n number of sequential chunks from l."""
        d, r = divmod(len(l), n)
        for i in range(n):
            si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
            yield l[si:si + (d + 1 if i < r else d)]


#####
wktfab = osmium.geom.WKTFactory()


class LimitRelationAreaHanlder(osmium.SimpleHandler):
    def __init__(self, relation_id):
        super().__init__()
        self.way_dict: Dict[Dict] = dict()
        self.relation_id = relation_id
        self.relation_dict = dict()

    def relation(self, relation):
        if relation.id == int(self.relation_id):
            for member in relation.members:
                if member.ref in self.way_dict and member.role == "outer" and member.type == "w":

                    if not self.relation_dict.get(relation.id, False):
                        self.relation_dict[relation.id] = []
                    self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name"), "geometry": way_geometry}


class LimitAreaUtils:
    @staticmethod
    def get_limit_relation_geom(filepath, relation_id):
        handler = LimitRelationAreaHanlder(relation_id)
        handler.apply_file(filepath, idx="flex_mem", locations=True)
        way_dict = handler.way_dict
        relation_dict = handler.relation_dict
        relation_member_dict = RingUtils.get_relation_member_data(relation_dict=relation_dict, way_dict=way_dict, tags=["outer", "inner", ""])
        relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict)
        relation_member_dict = relation_member_data.to_dict("index")
        relation_member_dict = RingUtils.restructure(relation_member_dict)
        relation_result = []
        polygon_id_used_table = []
        for relation_id, relation in relation_member_dict.items():
            logging.debug(f"Relation: {relation_id} doing merge.")

            outers = relation.get("outer")
            if outers:
                outers = RingUtils.get_merged_rings(outers, polygon_id_used_table, "water")
                relation_member_dict[relation_id] = outers
                for outer in outers:
                    relation_result.append(outer)

        geom = MultiPolygon([Polygon(i.get("geometry")) for i in relation_result])

        logging.debug("Get limit relation area geometry completed.")
        return geom

    @staticmethod
    def get_relation_polygon_with_overpy(rel_id: str) -> MultiPolygon:
        api = overpy.Overpass()
        query_msg = f"""
        [out:json][timeout:25];
        rel({rel_id});
        out body;
        >;
        out skel qt; 
        """
        result = api.query(query_msg)
        lineStrings = []
        for key, way in enumerate(result.ways):
            linestring_coords = []
            for node in way.nodes:
                linestring_coords.append(Point(node.lon, node.lat))
            lineStrings.append(LineString(linestring_coords))

        merged = linemerge([*lineStrings])
        borders = unary_union(merged)
        polygons = MultiPolygon(list(polygonize(borders)))
        return polygons

    @staticmethod
    def prepare_data(data_df: geopandas.GeoDataFrame, intersection_polygon_wkt: str) -> geopandas.GeoDataFrame:
        intersects_geom = wkt.loads(intersection_polygon_wkt)
        if intersects_geom.type == "LineString":
            intersects_geom = intersects_geom.buffer(1/6371000/math.pi*180)
        intersects_series = geopandas.GeoSeries(intersects_geom)
        intersects_indices = list(data_df.sindex.query_bulk(intersects_series, predicate="intersects")[1])
        data_df = data_df.iloc[intersects_indices]
        return data_df

class BuildingUtils:

    @staticmethod
    def get_building_rings_merged_results(relation_member_dict, polygon_id_used_table,mode) -> tuple:
        # Outer then inner
        for relation_id, relation in relation_member_dict.items():
            logging.debug(f"Relation: {relation_id} doing merge.")

            outers = relation.get("outer")
            if outers:
                outers = RingUtils.get_merged_rings(outers, polygon_id_used_table,mode)
                relation_member_dict[relation_id]["outer"] = outers

            others = relation.get("other")
            if others:
                others = RingUtils.get_merged_rings(others, polygon_id_used_table,mode)
                relation_member_dict[relation_id]["other"] = others

            inners = relation.get("inner")
            if inners:
                inners = RingUtils.get_merged_rings(inners, polygon_id_used_table, mode)
                relation_member_dict[relation_id]["inner"] = inners

    # TODO: Optimize
    @staticmethod
    def get_merged_rings(rings: list, polygon_id_used_table: list, mode) -> List[Dict]:
        def get_merged_line(ring, merging_candidates: list, current_merged_ids):
            # Avoid merge with self
            current_merged_ids.append(ring["way_id"])

            merging = ring.get("geometry")
            if merging.type == "Polygon":
                return merging

            candidate = NotImplemented
            merging_index = 0
            while merging_index < len(merging_candidates):
                candidate = merging_candidates[merging_index]
                candidate = candidate.get("geometry")

                if candidate.type == "Polygon":
                    merging_index += 1
                    continue

                candidate_id = candidate.get("way_id")
                try:
                    if candidate.get('way_id') in current_merged_ids:
                        merging_index += 1
                    else:
                        if is_reverse_needed(merging, candidate):
                            # Reverse the line and do merge with current index again.
                            logging.debug(f"candidate {candidate_id} reversed.")
                            candidate = reverse_linestring_coords(candidate)
                        if is_continuous(merging, candidate):
                            logging.debug(f"{ring.get('way_id')} merge with {candidate_id}")
                            # merge and start new round of iteration.
                            merging = linemerge_by_wkt(merging, candidate)
                            current_merged_ids.append(candidate_id)
                            merging_index = 0
                        else:
                            merging_index += 1
                except:
                    print(f"{candidate.get('way_id')} has some problems.")
            logging.debug(f"Return {merging}")
            return merging

        #################################################################################
        # Deep copy with merge candidate.
        merging_candidate = [ring for ring in rings]
        result = []
        current_merged_ids = []
        for ring in rings:
            # If being merged, skip it
            if ring.get('way_id') in current_merged_ids:
                continue

            logging.debug(f"WAY:{ring.get('way_id')} start doing merge.")
            merged_line = get_merged_line(ring, merging_candidate, current_merged_ids)

            # Choose way_id from merged line.
            for merged_id in current_merged_ids:
                if merged_id not in polygon_id_used_table:
                    logging.debug(f"{merged_id} is choosed as polygon id.")
                    # result.append(Building(polygon_id=merged_id, polygon_name=ring.get("name"),height=ring.get("height"), level=ring.get("level"), geometry=merged_line))
                    result.append({'POLYGON_ID': merged_id, "POLYGON_NAME": ring.get("name"), "height": ring.get("height"), "level": ring.get("level"), "geometry": merged_line})
                    polygon_id_used_table.append(merged_id)
                    break

        return result
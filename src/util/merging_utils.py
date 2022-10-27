import logging
import math
import multiprocessing
import os
import time
import traceback
from copy import deepcopy
from itertools import islice
from typing import Dict, List
import geopandas
import numpy
import overpy
import pandas
import shapely.ops
from shapely.geometry import LineString, Polygon, Point
from shapely.ops import linemerge, polygonize
from src.enum.hofn_type import HofnType


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

    @staticmethod
    def merged_current_level_ways(unmerged_level_road):
        unmerged = unmerged_level_road
        result = []
        unmerged_in_current_level: dict = unmerged.set_index(unmerged["POLYGON_ID"]).to_dict('index')
        unmerged_values = list(unmerged_in_current_level.values())

        i = len(unmerged_values) - 1
        j = i - 1
        while len(unmerged_values) > 0:
            mainline = unmerged_values[i]["geometry"]
            candidate = unmerged_values[j]["geometry"]
            if is_reverse_needed(mainline, candidate):
                candidate = reverse_linestring_coords(candidate)
            if is_continuous(mainline, candidate):
                logging.debug(f"{unmerged_values[i]['POLYGON_ID']} merged with {unmerged_values[j]['POLYGON_ID']}.")
                mainline = linemerge_by_wkt(mainline, candidate)
                unmerged_values[i]["geometry"] = mainline
                unmerged_values.pop(j)
                i = len(unmerged_values) - 1

                j = i - 1
            else:
                j = j - 1

            if j < 0:
                result.append(unmerged_values[i])
                unmerged_values.pop(i)
                i = len(unmerged_values) - 1
                j = i - 1
                if i > 0:
                    logging.debug(f"{unmerged_values[i]['POLYGON_ID']} start merge.")

                if i == 0:
                    result.append(unmerged_values[i])
                    unmerged_values.pop(i)
        return result

    @staticmethod
    def get_merged_and_divided(geometry_dict, origin_line, length_threshold):
        unmerged_keys = list(geometry_dict.keys())
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
    def merge_level_ways(unmergeds, cpu_count):

        def mp_merged(data_gdfs, using_cpu_count):
            pool = multiprocessing.Pool(using_cpu_count)
            tmp_results = pool.map(LineUtils.merged_current_level_ways, data_gdfs)
            pool.close()
            return tmp_results

        result = []
        for unmerged in unmergeds:
            using_cpu_count = cpu_count
            unmerged_gdfs = numpy.array_split(unmerged, using_cpu_count)
            unmerged_list = NotImplemented
            while using_cpu_count > 0 :
                logging.info(f"Current working -> LEVEL: {unmerged_gdfs[0].iloc[0]['ROAD_LEVEL']}, WAITING FOR MERGE COUNTS: {using_cpu_count}")
                unmerged_list = mp_merged(unmerged_gdfs, using_cpu_count)
                
                if using_cpu_count != 1:
                # merge
                    unmerged_gdfs = [geopandas.GeoDataFrame(unmerged_list[i] + unmerged_list[i + 1]) for i in range(0, len(unmerged_list) - 1, 2)]
                # Check if lost the last dataframe
                    if len(unmerged_list) % 2 == 1:
                        unmerged_gdfs[-1] = pandas.concat([unmerged_gdfs[-1], geopandas.GeoDataFrame(unmerged_list[-1])])
                    
                    using_cpu_count = len(unmerged_gdfs)
                else:
                    using_cpu_count = 0
            logging.info(f"LEVEL {unmerged_gdfs[0].iloc[0]['ROAD_LEVEL']} merged completed.")
            result += unmerged_list[0]
        logging.info("Merge done.")
        return result

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


#################################################
class RingUtils:
    @staticmethod
    def get_relation_member_data(relation_dict: Dict, way_dict: Dict, tags: list) -> list:
        result = []
        for relation_id, members in relation_dict.items():
            for member in members:
                ring_rel_members = {"relation_id": None, "way_id": None, "name": "UNKNOWN", "geometry": None, "role": None, "type": None}
                # Check if tags is in need. if not then pass.
                if member.get("role") not in tags:
                    continue
                # Using member id from relation dict to search in way dict.
                way_id = member.get("id")
                way = way_dict.get(way_id, False)
                if way is False:
                    logging.debug(f"{way_id} is None in way_dict")
                    continue

                role = member.get("role")
                # Discussed with Sheldon, if member's role is empty, regarded member as outer.
                if role == "":
                    role = "outer"
                    logging.debug(f"{relation_id}: {way_id} has empty role, regarded as OUTER.")

                if way:
                    ring_rel_members["relation_id"] = relation_id
                    ring_rel_members["way_id"] = way_id
                    ring_rel_members["name"] = way.get("name") if way.get("name") else "UNKNOWN"
                    ring_rel_members["geometry"] = way.get("geometry")
                    ring_rel_members["role"] = role
                    ring_rel_members["type"] = member.get("type")
                    result.append(ring_rel_members)
                else:
                    logging.debug(f"{way_id} cannot be found in way dict, please check.")
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
            current_merged_ids.append(ring["way_id"])

            merging_line = ring.get("geometry")
            candidate_line = NotImplemented
            merging_index = 0
            while merging_index < len(merging_candidates):
                candidate = merging_candidates[merging_index]
                candidate_line = candidate.get("geometry")
                candidate_id = candidate.get("way_id")
                try:
                    if candidate.get('way_id') in current_merged_ids:
                        merging_index += 1
                    else:
                        if is_reverse_needed(merging_line, candidate_line):
                            # Reverse the line and do merge with current index again.
                            logging.debug(f"candidate {candidate_id} reversed.")
                            candidate_line = reverse_linestring_coords(candidate_line)
                        if is_continuous(merging_line, candidate_line):
                            logging.debug(f"{ring.get('way_id')} merge with {candidate_id}")
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
            if ring.get('way_id') in current_merged_ids:
                continue

            logging.debug(f"WAY:{ring.get('way_id')} start doing merge.")
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

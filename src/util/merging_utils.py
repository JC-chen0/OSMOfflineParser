import logging
import math
import os
import time
import traceback
from copy import deepcopy
from typing import Dict, List
import geopandas
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
    return (head == compare_tail or tail == compare_head) and line1 != line2


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_head or tail == compare_tail


def lonlat_length_in_km(geom):
    return geom.length * 6371 * math.pi / 180


####################################################################################


def merged_level_roads(unmerged_level_road):
    unmerged = unmerged_level_road
    result = dict()
    logging.info(f"Start process level = {unmerged.iloc[0]['ROAD_LEVEL']}")
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
            result[f"{unmerged_values[i]['POLYGON_ID']}"] = unmerged_values[i]
            unmerged_values.pop(i)
            i = len(unmerged_values) - 1
            j = i - 1
            if i > 0:
                logging.debug(f"{unmerged_values[i]['POLYGON_ID']} start merge.")

            if i == 0:
                result[f"{unmerged_values[i]['POLYGON_ID']}"] = unmerged_values[i]
                unmerged_values.pop(i)
    return result


def get_merged_and_divided_by_threshold(geometry_dict, dividing_result_dict, tolerance, length_threshold) -> Dict:
    result_dict = dividing_result_dict
    compare_geometry_dict = deepcopy(geometry_dict)
    reach_length_limit_list = []
    start_id = next(iter(compare_geometry_dict))
    start_line = compare_geometry_dict.get(start_id).get("geometry")
    merging = True
    last_segment = False
    count = 0
    while merging:
        for compare_poly_id, compare_poly_dict in compare_geometry_dict.items():
            compare_geometry = compare_poly_dict["geometry"]

            if compare_poly_id in reach_length_limit_list and not last_segment:

                if len(compare_geometry_dict.keys()) - len(reach_length_limit_list) == 1:
                    last_segment = True
                continue

            if start_id == compare_poly_id:
                continue

            if is_reverse_needed(start_line, compare_geometry):
                compare_geometry = reverse_linestring_coords(compare_geometry)

            elif is_continuous(start_line, compare_geometry):
                merge_linestring = linemerge_by_wkt(start_line, compare_geometry)

                if last_segment:
                    # If last segment of line is too long, it actually doesn't need to be merged.
                    if start_line.length * 6371 * math.pi / 180 > tolerance:
                        merging = False

                    compare_geometry_dict.get(start_id)["geometry"] = merge_linestring
                    result_dict.get("POLYGON_ID").append(compare_geometry_dict.get(start_id)["POLYGON_ID"])
                    result_dict.get("POLYGON_NAME").append(compare_geometry_dict.get(start_id)["POLYGON_NAME"])
                    result_dict.get("geometry").append(start_line)
                    result_dict.get("HOFN_TYPE").append(compare_geometry_dict.get(start_id)["HOFN_TYPE"])
                    result_dict.get("ROAD_LEVEL").append(compare_geometry_dict.get(start_id)["ROAD_LEVEL"])
                    merging = False

                elif merge_linestring.length * 6371 * math.pi / 180 >= length_threshold:
                    reach_length_limit_list.append(start_id)
                    compare_geometry_dict.get(start_id)["geometry"] = start_line
                    result_dict.get("POLYGON_ID").append(compare_geometry_dict.get(start_id)["POLYGON_ID"])
                    result_dict.get("POLYGON_NAME").append(compare_geometry_dict.get(start_id)["POLYGON_NAME"])
                    result_dict.get("geometry").append(start_line)
                    result_dict.get("HOFN_TYPE").append(compare_geometry_dict.get(start_id)["HOFN_TYPE"])
                    result_dict.get("ROAD_LEVEL").append(compare_geometry_dict.get(start_id)["ROAD_LEVEL"])
                    # Restart looping
                    logging.debug(f"{start_id} reach length limit. Next start line will use {compare_poly_id}")
                    start_id = compare_poly_id
                    start_line = compare_geometry
                    break

                start_line = merge_linestring
                # remove merged id
                compare_geometry_dict.pop(compare_poly_id)
                logging.debug(f"{start_id} merge with {compare_poly_id}, break, {compare_poly_id} will be removed.")
                break
    logging.debug(f"Re-merge and dividing completed.")
    return result_dict


def linemerge_by_wkt(line1, line2) -> LineString:
    line1_coords = line1.coords[:]
    line2_coords = line2.coords[:]
    source, target = (line1, line2) if line1_coords[-1] == line2_coords[0] else (line2, line1)
    coords = source.coords[:]
    coords.pop(-1)
    coords.extend(target.coords[:])
    new_linestring = LineString(coords)
    return new_linestring


def filter_small_island(merged: dict, area_threshold: int):
    #  filter the small island, where there is no people
    filtered = merged
    small_island_list = []
    for key, values in merged.items():
        try:
            geometry = values["geometry"]
            if list(polygonize(geometry))[0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
                small_island_list.append(key)
        except:
            logging.debug(f"POLYGON_ID: {key} cannot be polygonized.")
    [filtered.pop(key) for key in small_island_list]
    return filtered


#################################################
# RINGS
def get_relation_member_data(relation_dict: Dict, way_dict: Dict, tags: list) -> Dict:
    ring_rel_members_dict = {"relation_id": [], "way_id": [], "name": [], "geometry": [], "role": [], "type": []}

    for relation_id, members in relation_dict.items():
        for member in members:
            if member.get("role") not in tags:
                continue
            way_id = member.get("id")
            way = way_dict.get(way_id, False)
            if way is False:
                logging.debug(f"{way_id} is None in way_dict")
                continue
            name = way.get("name")
            role = member.get("role")
            if role == "":
                role = "outer"
                logging.debug(f"{relation_id}: {way_id} has empty role, regarded as OUTER.")
            if way:
                ring_rel_members_dict.get("relation_id").append(relation_id)
                ring_rel_members_dict.get("way_id").append(way_id)
                ring_rel_members_dict.get("name").append(way.get("name"))
                ring_rel_members_dict.get("geometry").append(way.get("geometry"))
                ring_rel_members_dict.get("role").append(role)
                ring_rel_members_dict.get("type").append(member.get("type"))
            else:
                logging.debug(f"{way_id} cannot be found in way dict, please check.")
    return ring_rel_members_dict


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


def inners_extracting(inners: List[Dict], islands: List[Dict]):
    for inner in inners:
        append = {"POLYGON_ID": inner.get("POLYGON_ID"),
                  "POLYGON_NAME": inner.get("POLYGON_NAME"),
                  "geometry": inner.get("geometry"),
                  "HOFN_TYPE": "5",
                  "ROAD_LEVEL": "0"}
        islands.append(append)


def get_merged_rings(rings: list, polygon_id_used_table: list, mode) -> List[Dict]:
    ############ INLINE FUNCTION ########
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
                result.append({'POLYGON_ID': merged_id, "POLYGON_NAME": ring.get("name"), "geometry": merged_line, "HOFN_TYPE": HofnType[mode].value, "ROAD_LEVEL": 0})
                polygon_id_used_table.append(merged_id)
                break
            else:
                logging.debug(f"{merged_id} has been used, change another polygon id")

    return result


def polygonize_with_try_catch(row, remove_list):
    try:
        return list(shapely.ops.polygonize(row["geometry"]))[0]
        # return Polygon(row["geometry"])
    except:
        logging.debug(f"{row['POLYGON_ID']} cannot be polygonized, geometry is {row['geometry']}, return origin LINESTRING instead")
        remove_list.append(row['POLYGON_ID'])
        return row["geometry"]


def remove_within_outer(rings: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    def is_within(row, compare: geopandas.GeoSeries, current_index):
        try:
            polygons = compare.drop(current_index)
            within = polygons.contains(row["geometry"])
            if True in list(within):
                logging.debug(f"{row['POLYGON_ID']} will be removed due to within other polygon.")
                return 1
            return 0
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot do contains.")
            traceback.print_exc()

    start_time = time.time()
    compare = rings["geometry"]
    for index, row in rings.iterrows():
        rings.at[index, "within"] = is_within(row, compare, index)
    extract = rings[rings["within"] != 1]
    logging.debug(f"remove within outer, taking {time.time() - start_time}")
    extract = extract.drop(columns=["within"])
    return extract


def remove_over_intersection_outer(rings: geopandas.GeoDataFrame) -> geopandas.GeoDataFrame:
    def is_over_intersect(row, compare: geopandas.GeoSeries, current_index):
        try:
            geometry = row["geometry"]
            polygons = compare.drop(current_index)
            intersects = polygons.intersects(geometry)
            if True in list(intersects):
                true_indexes = [i for i, x in intersects.iteritems() if x]
                for index in true_indexes:
                    intersect_polygon = geometry.difference(polygons[index])
                    cover_percentage = (intersect_polygon.area / geometry.area) * 100
                    if cover_percentage < 3:
                        print(f"{row['POLYGON_ID']} will be removed due to over_intersection")
                        return 1
            return 0
        except:
            logging.debug(f"{row['POLYGON_ID']} cannot do intersect.")
            traceback.print_exc()

    start_time = time.time()
    compare = rings["geometry"]
    rings["over_intersect"] = 0
    for index, row in rings.iterrows():
        rings.at[index, "over_intersect"] = is_over_intersect(row, compare, index)
    extract = rings[rings["over_intersect"] != 1]
    extract = extract.drop(columns=["over_intersect"])
    print(f"remove within outer, taking {time.time() - start_time}")
    return extract


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

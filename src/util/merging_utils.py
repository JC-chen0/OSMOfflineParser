import logging
import math

import geopandas
from copy import deepcopy
from typing import Dict
from geopandas import GeoDataFrame
from shapely import wkt
from shapely.ops import linemerge, polygonize


def reverse_linestring_coords(geometry):
    geometry.coords = list(geometry.coords)[::-1]


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


def merge_with_candidates_dict(row, unmerged_ids, result, candidates: dict):
    # row, unmerged_ids: do traversal and merge
    # unmerged: final merged result
    # candidates: merging candidate
    logging.debug(f"{row['POLYGON_ID']} start merging.")
    candidates_ids = list(candidates.keys())
    candidates_values = list(candidates.values())
    current_merging = row["POLYGON_STR"]
    # Use reverse traversal to remove merged id
    i = len(candidates) - 1
    while i >= 0:
        candidate_id = candidates_ids[i]
        candidate_value = candidates_values[i]
        candidate_geometry = candidate_value["POLYGON_STR"]
        if is_reverse_needed(current_merging, candidate_geometry):
            reverse_linestring_coords(candidate_geometry)
            logging.debug(f"{candidate_id} reversed.")
        if is_continuous(current_merging, candidate_geometry):
            current_merging = linemerge([current_merging, candidate_geometry])
            result[row["POLYGON_ID"]]["POLYGON_STR"] = current_merging
            logging.debug(f"{row['POLYGON_ID']} merge with {candidate_id}")
            # Candidate has been merged.
            candidates_ids.remove(candidate_id)
            candidates_values.remove(candidate_value)
            candidates.pop(candidate_id)

            unmerged_ids.remove(candidate_id)
            result.pop(candidate_id)

            i = len(candidates_ids) - 1
        else:
            i -= 1
    return row


def merge_with_candidates(row, candidates: dict, processed_ids: list):
    if row["POLYGON_ID"] in processed_ids:
        row["POLYGON_STR"] = None
        return
    if row["POLYGON_ID"] in candidates.keys():
        candidates.pop(row["POLYGON_ID"])

    if row["POLYGON_ID"] == 1078983773:
        print("hi")
    logging.debug(f"{row['POLYGON_ID']} start merging.")

    i = len(candidates) - 1
    candidates_ids = list(candidates.keys())
    candidates_values = list(candidates.values())
    while i >= 0:
        candidate_id = candidates_ids[i]
        candidate_value = candidates_values[i]
        delete_id = None
        candidate_geometry = candidate_value["POLYGON_STR"]
        if is_reverse_needed(row["POLYGON_STR"], candidate_geometry):
            reverse_linestring_coords(candidate_geometry)
            logging.debug(f"{candidate_id} reversed.")
        if is_continuous(row["POLYGON_STR"], candidate_geometry):
            row["POLYGON_STR"] = linemerge([row["POLYGON_STR"], candidate_geometry])
            processed_ids.append(candidate_id)

            logging.debug(f"{row['POLYGON_ID']} merge with {candidate_id}")
            candidates_ids.remove(candidate_id)
            candidates_values.remove(candidate_value)
            i = len(candidates_ids) - 1
        i -= 1
    return row

    ####################################################################################


def get_merged_and_divided_by_threshold(geometry_dict, tolerance, length_threshold) -> Dict:
    compare_geometry_dict = deepcopy(geometry_dict)
    reach_length_limit_list = []
    start_id = next(iter(compare_geometry_dict))
    start_line = compare_geometry_dict.get(start_id).get("POLYGON_STR")
    merging = True
    last_segment = False
    count = 0
    while merging:
        for compare_poly_id, compare_poly_dict in compare_geometry_dict.items():
            compare_geometry = compare_poly_dict["POLYGON_STR"]

            if compare_poly_id in reach_length_limit_list and not last_segment:

                if len(compare_geometry_dict.keys()) - len(reach_length_limit_list) == 1:
                    last_segment = True
                continue

            if start_id == compare_poly_id:
                continue

            if is_reverse_needed(start_line, compare_geometry):
                compare_geometry = reverse_linestring_coords(compare_geometry)

            elif is_continuous(start_line, compare_geometry):
                merge_linestring = linemerge([start_line, compare_geometry])

                if last_segment:
                    # If last segment of line is too long, it actually doesn't need to be merged.
                    if start_line.length * 6371 * math.pi / 180 > tolerance:
                        merging = False

                    compare_geometry_dict.get(start_id)["POLYGON_STR"] = merge_linestring
                    merging = False

                elif merge_linestring.length * 6371 * math.pi / 180 >= length_threshold:
                    reach_length_limit_list.append(start_id)
                    compare_geometry_dict.get(start_id)["POLYGON_STR"] = start_line
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
    return compare_geometry_dict


def prepare_data(data_df: GeoDataFrame, intersection_polygon_wkt: str, geometry_column: str) -> GeoDataFrame:
    geometries = data_df[geometry_column]
    polygon = wkt.loads(intersection_polygon_wkt)
    data_df["in_polygon"] = geometries.intersects(polygon)
    data_df = data_df[data_df["in_polygon"]]
    del data_df["in_polygon"]
    return data_df


def read_file_and_rename_geometry(file_path: str):
    tmp = geopandas.read_file(file_path)
    tmp.rename_geometry("POLYGON_STR", inplace=True)
    return tmp


def filter_small_island(merged: dict, area_threshold: int):
    #  filter the small island, where there is no people
    filtered = merged
    small_island_list = []
    for key, values in merged.items():
        try:
            geometry = values["POLYGON_STR"]
            if list(polygonize(geometry))[0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
                small_island_list.append(key)
        except:
            logging.debug(f"POLYGON_ID: {key} cannot be polygonized.")
    [filtered.pop(key) for key in small_island_list]
    return filtered
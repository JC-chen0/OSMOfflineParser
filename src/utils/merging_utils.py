import logging
import math
import time
import geopandas
import overpy
from copy import deepcopy
from typing import Dict
from geopandas import GeoDataFrame, GeoSeries
from shapely import wkt
from shapely.geometry import LineString, MultiPolygon, Point
from shapely.ops import linemerge, unary_union, polygonize


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



# TODO: Tuning performance
def get_merged(unmerged_highways_df: GeoDataFrame) -> Dict:
    highway_dict = unmerged_highways_df.set_index(unmerged_highways_df["POLYGON_ID"]).to_dict('index')
    highway_merge_dict = deepcopy(highway_dict)
    start_time = time.time()
    processed_list = []
    for highway_id, highway_dict in highway_dict.items():
        logging.debug(f"{highway_id} start merging")
        if highway_id in processed_list:
            continue
        geometry = highway_dict['POLYGON_STR']
        level = highway_dict["HOFN_LEVEL"]
        merging = True
        while merging:
            for compare_id, compare_highway in highway_merge_dict.items():
                compare_geometry = compare_highway["POLYGON_STR"]
                compare_level = compare_highway["HOFN_LEVEL"]

                if compare_id == list(highway_merge_dict.keys())[-1]:
                    logging.debug(f"{highway_id} merge process completed, start another round.")
                    merging = False
                    break

                if level != compare_level or highway_id == compare_id:
                    continue

                ############################################
                if is_reverse_needed(geometry, compare_geometry):
                    compare_geometry = reverse_linestring_coords(compare_geometry)
                elif is_continuous(geometry, compare_geometry):
                    merge_linestring = linemerge([compare_geometry, geometry])
                    geometry = merge_linestring
                    highway_merge_dict.get(highway_id)["POLYGON_STR"] = geometry
                    # remove merged id
                    highway_merge_dict.pop(compare_id)
                    processed_list.append(compare_id)
                    logging.debug(f"{highway_id} merge with {compare_id}, break, {compare_id} will be removed.")
                    break
    return highway_merge_dict


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


# TODO: Tuning performance using multiprocess
def prepare_data(data_df: GeoDataFrame, intersection_polygon_wkt: str, geometry_column: str) -> GeoDataFrame:
    geometries = data_df[geometry_column]
    polygon = wkt.loads(intersection_polygon_wkt)
    data_df["in_polygon"] = geometries.intersects(polygon)
    data_df = data_df[data_df["in_polygon"]]
    del data_df["in_polygon"]
    return data_df


def get_relation_polygon(rel_id: str) -> MultiPolygon:
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


def read_file_and_rename_geometry(file_path: str):
    tmp = geopandas.read_file(file_path)
    tmp.rename_geometry("POLYGON_STR", inplace=True)
    return tmp

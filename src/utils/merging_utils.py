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


def reverse_linestring_coords(geometry) -> LineString:
    geometry.coords = list(geometry.coords)[::-1]


def is_continuous(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_tail or tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return (head == compare_head and tail != compare_tail) or (tail == compare_tail and head != compare_head)


def lonlat_length_in_km(geom):
    return geom.length * 6371 * math.pi / 180


# TODO: Tuning performance
def get_merged(unmerged_highways_df: GeoDataFrame) -> Dict:
    highway_dict = unmerged_highways_df.set_index(unmerged_highways_df["POLYGON_ID"]).to_dict('index')
    highway_merge_dict = deepcopy(highway_dict)
    start_time = time.time()
    processed_list = []
    for highway_id, highway_dict in highway_dict.items():
        print(f"{highway_id} start merging")
        if highway_id in processed_list:
            continue
        geometry = highway_dict['geometry']
        level = highway_dict["HOFN_LEVEL"]
        merging = True
        while merging:
            for compare_id, compare_highway in highway_merge_dict.items():
                compare_geometry = compare_highway["geometry"]
                compare_level = compare_highway["HOFN_LEVEL"]

                if compare_id == list(highway_merge_dict.keys())[-1]:
                    print(f"{highway_id} merge process completed, start another round.")
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
                    highway_merge_dict.get(highway_id)["geometry"] = geometry
                    # remove merged id
                    highway_merge_dict.pop(compare_id)
                    processed_list.append(compare_id)
                    print(f"{highway_id} merge with {compare_id}, break, {compare_id} will be removed.")
                    break
    return highway_merge_dict


def get_merged_and_divided_by_threshold(geometry_dict, tolerance, length_threshold) -> Dict:
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
                merge_linestring = linemerge([start_line, compare_geometry])

                if last_segment:
                    # If last segment of line is too long, it actually doesn't need to be merged.
                    if start_line.length * 6371 * math.pi / 180 > tolerance:
                        merging = False

                    compare_geometry_dict.get(start_id)["geometry"] = merge_linestring
                    merging = False

                elif merge_linestring.length * 6371 * math.pi / 180 >= length_threshold:
                    reach_length_limit_list.append(start_id)
                    compare_geometry_dict.get(start_id)["geometry"] = start_line
                    # Restart looping
                    print(f"{start_id} reach length limit. Next start line will use {compare_poly_id}")
                    start_id = compare_poly_id
                    start_line = compare_geometry
                    break

                start_line = merge_linestring
                # remove merged id
                compare_geometry_dict.pop(compare_poly_id)
                print(f"{start_id} merge with {compare_poly_id}, break, {compare_poly_id} will be removed.")
                break
    return compare_geometry_dict


# TODO: Tuning performance using multiprocess
def prepare_data(data_df: GeoDataFrame, intersection_polygon_wkt: str) -> GeoDataFrame:

    geometries = data_df["POLYGON_STR"]
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


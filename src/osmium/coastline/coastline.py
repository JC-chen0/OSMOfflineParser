from copy import deepcopy
from typing import List, Dict
import shapely
from shapely import wkt
import geopandas
import time
import re
from shapely.geometry import Point, LineString, MultiLineString

# 海岸線作業
# 1. 本島海岸線，海岸線每一百公里sep一段，取其中一段的way_id作為海岸線一百公里的ID(Rule1)
# 2. 離島要是發現有一些面積太小的，忽略他(Rule2)
# 3. 假設台灣的海岸線總共1411公里，理論上分成15段，最後一段11公里，但現在換成14段，
#    最後一段特別延伸，不要讓最後一段特別短
coastline_df = geopandas.read_file("data/input/general_coastline_taiwan.geojson")
coastline_dict = coastline_df.set_index(coastline_df["id"]).to_dict('index')
coastline_merge_dict = deepcopy(coastline_dict)


def reverse_linestring_by_wkt(geometry_wkt: str) -> LineString:
    split = (re.split("\\(|,|\\)", geometry_wkt))
    split = split[::-1]
    split = [point.strip() for point in split]

    head_point_idx = 1
    tail_point_idx = -1
    split = split[head_point_idx:tail_point_idx]

    join = ",".join(split)
    join = "LINESTRING (" + join + ")"
    result = wkt.loads(join)
    return result


def reverse_linestring_coords(geometry) -> LineString:
    geometry.coords = list(geometry.coords)[::-1]


def is_continuous(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_tail and tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_head and tail != compare_tail or tail == compare_tail and head != compare_head


# %% for loop method: Not completed iteration.

start_time = time.time()
processed_list = []
for id, coastline in coastline_dict.items():
    if id in processed_list:
        continue
    geometry = coastline["geometry"]
    merging_process = True
    while merging_process:
        skip_id = 0
        for sub_id, sub_coastline in coastline_merge_dict.items():
            compare_geometry = sub_coastline["geometry"]
            print(is_reverse_needed(geometry, compare_geometry))
            if id == sub_id:
                continue
            if is_reverse_needed(geometry, compare_geometry):
                print(compare_geometry.wkt)
                reverse_linestring_coords(compare_geometry)
                print(compare_geometry.wkt)
            if is_continuous(geometry, compare_geometry):
                merge_linestring = shapely.ops.linemerge([compare_geometry, geometry])
                geometry = merge_linestring
                processed_list.append(sub_id)
                skip_id = sub_id
                merging_process = False
                print("continuous")
                break

            if sub_id == list(coastline_merge_dict.keys())[-1]:
                merging_process = False

        coastline_merge_dict[id]["geometry"] = geometry
        coastline_merge_dict.pop(skip_id, 0)

print(f"processed time {time.time() - start_time} seconds")
coastline_merge_dataframe = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
coastline_merge_dataframe.to_file("data\\output\\coastline_merge.geojson", driver="GeoJSON")

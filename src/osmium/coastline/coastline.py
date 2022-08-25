from copy import deepcopy
from typing import List, Dict
import shapely
from shapely import wkt, ops
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

taiwan_polygon = wkt.loads(
    "POLYGON((-243.33618164062506 20.468189222640945,-238.07373046875003 21.75949973071981,-238.2110595703125 23.458207269894118,-237.7056884765625 24.941238299396304,-237.62878417968753 25.725683932942644,-237.8924560546875 25.874051998991945,-239.0020751953125 25.28940455649483,-240.12817382812497 24.397133017391056,-240.89172363281244 23.64955612214773,-240.96313476562491 23.1403599878861,-240.523681640625 21.922663209325933,-241.47468566894534 24.50027045676029,-241.56257629394534 24.540252541034334,-241.6044616699219 24.545874000679646,-241.6648864746094 24.514015733632306,-241.72256469726565 24.511516704199963,-241.7823028564453 24.473400349324066,-241.86195373535148 24.41088955100095,-241.87105178833008 24.380247951256834,-241.79397583007804 24.316439578554778,-241.82178497314453 24.176198731942094,-241.79260253906244 24.086589258228045,-243.54492187499991 20.81774101978648,-243.33618164062506 20.468189222640945))")


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
    return head == compare_tail or tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return (head == compare_head and tail != compare_tail) or (tail == compare_tail and head != compare_head)


# %% for loop method: Not completed iteration.

# start_time = time.time()
# processed_list = []
# for id, coastline in coastline_dict.items():
#     if id in processed_list:
#         continue
#     geometry = coastline["geometry"]
#     del_list = []
#     for sub_id, sub_coastline in coastline_merge_dict.items():
#         compare_geometry = sub_coastline["geometry"]
#
#         if id == sub_id:
#             continue
#
#         if is_reverse_needed(geometry, compare_geometry):
#             compare_geometry = reverse_linestring_coords(compare_geometry.wkt)
#
#         if is_continuous(geometry, compare_geometry):
#             merge_linestring = shapely.ops.linemerge([compare_geometry, geometry])
#             geometry = merge_linestring
#             processed_list.append(sub_id)
#             del_list.append(sub_id)
#
#     coastline_merge_dict[id]["geometry"] = geometry
#     [coastline_merge_dict.pop(del_id) for del_id in del_list]
#
# print(f"processed time {time.time() - start_time} seconds")
# %%
start_time = time.time()
processed_list = []
for id, coastline in coastline_dict.items():
    if id in processed_list:
        continue
    geometry = coastline["geometry"]
    merging = True
    del_list = []
    while merging:
        for sub_id, sub_coastline in coastline_merge_dict.items():
            compare_geometry = sub_coastline["geometry"]

            if id == sub_id:
                continue

            if is_reverse_needed(geometry, compare_geometry):
                compare_geometry = reverse_linestring_coords(compare_geometry.wkt)

            if is_continuous(geometry, compare_geometry):
                merge_linestring = shapely.ops.linemerge([compare_geometry, geometry])
                geometry = merge_linestring
                processed_list.append(sub_id)
                del_list.append(sub_id)
                coastline_merge_dict[id] = geometry
                break  # break inner for loop to start another round of merging

            if sub_id == coastline_merge_dict.keys()[-1]:
                merging = False


# %%
df = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
df = df[df.apply(lambda x: bool(taiwan_polygon.intersection(x.geometry)), axis=0)]
# df.to_file("data\\output\\coastline_merge.geojson", driver="GeoJSON")

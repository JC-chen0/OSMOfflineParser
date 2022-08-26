import math
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

origin_coastline_dict = geopandas.read_file("data\\input\\general_coastline_taiwan.geojson")


# intersect with taiwan
def prepare_data(file_path: str, intersection_polygon_wkt: str) -> Dict:
    coastline_df = geopandas.read_file(file_path)
    polygon = wkt.loads(intersection_polygon_wkt)
    coastline_df["in_polygon"] = coastline_df.apply(lambda x: bool(polygon.intersection(x.geometry)), axis=1)
    coastline_df = coastline_df[coastline_df["in_polygon"]]
    del coastline_df["in_polygon"]
    coastline_dict = coastline_df.set_index(coastline_df["id"]).to_dict('index')
    return coastline_dict


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


def get_merge_coastline(coastline_dict, dividing: bool = False) -> Dict:
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

    def lonlat_length_in_meters(length):
        return length * 6371000 * math.pi / 180

    LENGTH_THRESHOLD = 100000  # meters
    TOLERANCE = 10000  # meters
    coastline_merge_dict = deepcopy(coastline_dict)
    start_time = time.time()
    processed_list = []
    for id, coastline in coastline_dict.items():
        if id in processed_list:
            continue
        geometry = coastline["geometry"]
        merging = True
        del_id = NotImplemented
        print("=================================")
        print(f"{id} start merging.")
        while merging:
            for sub_id, sub_coastline in coastline_merge_dict.items():
                compare_geometry = sub_coastline["geometry"]

                if sub_id == list(coastline_merge_dict.keys())[-1]:
                    print(f"{sub_id} encounter the last item {list(coastline_merge_dict.keys())[-1]}")
                    merging = False
                    print(f"{id} merge process completed, start another round.")
                    break

                if id == sub_id:
                    continue

                if is_reverse_needed(geometry, compare_geometry):
                    compare_geometry = reverse_linestring_coords(compare_geometry.wkt)
                elif is_continuous(geometry, compare_geometry):
                    merge_linestring = shapely.ops.linemerge([compare_geometry, geometry])
                    # check if dividing in need. If so, check length.
                    if dividing:
                        pass
                    geometry = merge_linestring
                    coastline_merge_dict.get(id)["geometry"] = geometry

                    # remove merged id
                    del_id = sub_id
                    coastline_merge_dict.pop(del_id)

                    processed_list.append(sub_id)
                    print(f"{id} merge with {sub_id}, break, {sub_id} will be removed.")
                    break  # break inner for loop to start another round of merging
    print("=================================")
    print(f"Merging process complete, taking: {time.time() - start_time} seconds")
    return coastline_merge_dict


def filter_small_island(data, length_threshold):
    start_time = time.time()
    #  filter the small island, where there is no people
    del_ids = []
    for id, coastline in data.items():
        if coastline["geometry"].length * 6371000 * math.pi / 180 < length_threshold:
            del_ids.append(id)

    [data.pop(del_id) for del_id in del_ids]
    print("=================================")
    print(f"length filter process completed, taking: {time.time() - start_time} seconds")


# %%
taiwan_territorial_wkt = "POLYGON((122.31628417968749 25.755372398367243,122.48657226562499 24.041447460138727,121.94824218749999 21.708473013245978,117.14172363281249 20.257043804632374,116.22436523437501 20.41671698894571,116.3067626953125 21.2125797906306,120.55297851562501 21.637005211106327,119.76196289062501 22.522705703482472,118.8446044921875 23.3523425975811,119.1522216796875 23.629427267052435,118.19641113281249 24.066528197726868,118.15933227539061 24.289530340411346,118.18490982055665 24.335835720364003,118.16139221191408 24.353195877156622,118.13135147094725 24.379935243572717,118.12826156616208 24.401197603740286,118.15795898437497 24.419642928396627,118.19383621215823 24.439804648041587,118.23434829711914 24.484648999654027,118.27486038208009 24.50870523668064,118.34335327148438 24.52073162893086,118.4044647216797 24.545405555328742,118.45458984374999 24.530258213223604,118.52050781249999 24.500114251794585,118.54505538940428 24.46730691063459,118.55209350585935 24.41807986981408,118.4827423095703 24.322071022275992,119.39048767089842 25.04890260107915,119.5206069946289 25.066941137263314,119.56695556640624 24.95586875269946,121.92626953124999 25.88393659458397,122.31628417968749 25.755372398367243))"
coastline_dict = prepare_data(file_path="data\\input\\general_coastline_taiwan.geojson",
                              intersection_polygon_wkt=taiwan_territorial_wkt)
coastline_merge_dict = get_merge_coastline(coastline_dict)
filter_small_island(coastline_merge_dict, length_threshold=250)
df = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
df.to_file("data\\output\\coastline_merge.geojson", driver="GeoJSON")
print("=================================")
print("output process completed")
# %% mainland
mainland_wkt = "POLYGON((121.7329788208008 25.175116531621754,121.95476531982422 25.12912260815878,122.01690673828125 25.005972656239194,121.85211181640625 24.727498557349676,122.02789306640625 24.26449336927064,121.6351318359375 23.883326327031455,121.3385009765625 22.705255477207515,120.87432861328125 21.866596775776173,120.46234130859376 21.920115213557352,120.11901855468753 22.92298239536285,119.91027832031251 23.211058276488203,120.12537002563474 23.53424513266883,120.13481140136719 23.80670605941205,120.15008926391602 23.70866671996133,120.16656875610352 23.71196732092926,120.17120361328125 23.743554561188304,120.14699935913085 23.778275971021685,120.17875671386717 23.824923176051726,121.01715087890626 25.042059703222577,121.3165283203125 25.217365923668325,121.56234741210938 25.344026029134326,121.74636840820312 25.2161235037439,121.7329788208008 25.175116531621754))"
taiwan_mainland_coastline_dict = prepare_data("data\\output\\coastline_merge.geojson",
                                              intersection_polygon_wkt=mainland_wkt)
taiwan_mainland_coastline_merged_dict = get_merge_coastline(taiwan_mainland_coastline_dict)
taiwan_mainland_df = geopandas.GeoDataFrame.from_dict(taiwan_mainland_coastline_merged_dict, orient='index')
taiwan_mainland_df.to_file("data\\output\\mainland_taiwan.geojson", driver="GeoJSON")
print("=================================")
print("mainland output process completed")

# %%
taiwan_mainland_df = geopandas.read_file("data\\output\\mainland_taiwan.geojson")
taiwan_territorial_coastline_df = geopandas.read_file("data\\output\\coastline_merge.geojson")
taiwan_territorial_coastline_without_mainland = geopandas.overlay(taiwan_territorial_coastline_df, taiwan_mainland_df,
                                                                  how="difference")
taiwan_territorial_coastline_without_mainland.to_file("data\\output\\without_mainland.geojson", driver="GeoJSON")
result = geopandas.overlay(taiwan_territorial_coastline_without_mainland, taiwan_mainland_df, how="union")
result.to_file("data\\output\\taiwan_final.geojson", driver="GeoJSON")
print("Final result output completed.")

# %%
for coastline in taiwan_mainland_df.itertuples():
    print(f" {coastline.id}: {coastline.geometry.length * 6371000 * math.pi / 180}")

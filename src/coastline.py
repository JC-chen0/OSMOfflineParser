import math
import time

import geopandas
import osmium
import pandas
from geopandas import GeoDataFrame
from shapely import wkt
from shapely.ops import polygonize

from src.utils.merging_utils import get_relation_polygon, lonlat_length_in_km, prepare_data, get_merged, \
    get_merged_and_divided_by_threshold

# %%
wkt_factory = osmium.geom.WKTFactory()


class CoastlineHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.coastlines = {'POLYGON_ID': [], 'POLYGON_NAME': [], 'POLYGON_STR': [], "HOFN_TYPE": [], "HOFN_LEVEL": []}

    def way(self, w):
        self.get_coastlines(w)

    def get_coastlines(self, w):
        coastline_id = w.id
        coastline_name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        natural = w.tags.get("natural")
        if natural == "coastline":
            coastline = wkt.loads(wkt_factory.create_linestring(w))
            try:
                self.append_coastline_attribute(self.coastlines, coastline_id, coastline_name, coastline)
            except Exception as e:
                pass

    def append_coastline_attribute(self, attributes: dict, coastline_id: str, name, geometry):
        # http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage
        attributes["POLYGON_ID"].append(coastline_id)
        attributes["POLYGON_NAME"].append(name)
        attributes["POLYGON_STR"].append(geometry)
        attributes["HOFN_TYPE"].append(2)
        attributes["HOFN_LEVEL"].append(1)


coastline_handler = CoastlineHandler()
coastline_handler.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", idx="flex_mem", locations=True)
coastline_df = geopandas.GeoDataFrame(coastline_handler.coastlines, geometry="POLYGON_STR")
coastline_df.to_file("data\\output\\coastline\\unmerged_coastline.geojson", driver="GeoJSON")


def filter_small_island(data: dict, area_threshold: int):
    start_time = time.time()
    #  filter the small island, where there is no people
    del_ids = []
    for id, coastline in data.items():
        # will only have a polygon
        if list(polygonize(coastline.get("geometry")))[
            0].area * 6371000 * math.pi / 180 * 6371000 * math.pi / 180 < area_threshold:
            del_ids.append(id)

    [data.pop(del_id) for del_id in del_ids]
    print("=================================")
    print(f"length filter process completed, taking: {time.time() - start_time} seconds")


#  Merge all coastline
taiwan_territorial_geom = get_relation_polygon("449220")
coastline = prepare_data(file_path="data\\output\\coastline\\unmerged_coastline.geojson",
                         intersection_polygon_wkt=taiwan_territorial_geom.wkt)
coastline_merge_dict = get_merged(coastline)
filter_small_island(coastline_merge_dict, area_threshold=40000)
df = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
df["length"] = df.apply(lambda row: lonlat_length_in_km(row["geometry"]), axis=1)
df.to_file("data\\output\\coastline\\merged_coastline.geojson", driver="GeoJSON")
print("=================================")
print("Merging coastline process completed")

#  Merging taiwan mainland
merged_coastline: GeoDataFrame = geopandas.read_file("data\\output\\coastline\\merged_coastline.geojson")

mainland_coastline_wkt = list(merged_coastline.loc[merged_coastline["POLYGON_ID"] == 9406157]["geometry"])[0].wkt
mainland_coastline = prepare_data(file_path="data\\output\\coastline\\unmerged_coastline.geojson",
                                  intersection_polygon_wkt=mainland_coastline_wkt)
mainland_coastline_dict = mainland_coastline.set_index(mainland_coastline["POLYGON_ID"]).to_dict('index')
mainland_coastline_merged_result = get_merged_and_divided_by_threshold(mainland_coastline_dict, 60.0, 100.0)
mainland_result = geopandas.GeoDataFrame.from_dict(mainland_coastline_merged_result, orient="index")
mainland_result["length"] = mainland_result.apply(lambda row: lonlat_length_in_km(row["geometry"]), axis=1)
print("==================================")
print("Merging and dividing coastline process completed")

merged_coastline = merged_coastline[merged_coastline.POLYGON_ID != 9406157]
result = geopandas.GeoDataFrame(pandas.concat([merged_coastline, mainland_result], ignore_index=True))
result.to_file("data\\output\\coastline\\merged_and_divided_coastline.geojson", driver="GeoJSON")

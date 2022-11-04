# %%
import geopandas
import pandas
import overpy
import sys
sys.setrecursionlimit(2000)
from shapely.geometry import Point, LineString, Polygon
from src.utils import LineUtils, brute_force_merge
# NT2_GEO_POLYGON = pandas.read_csv(f"{FILE_PATH}/NT2_GEO_POLYGON.tsv", sep="\t")
# NT2_GEO_POLYGON.to_csv(f"{FILE_PATH}/NT2_GEO_POLYGON.csv", index=False)
COUNTRY = "Mexico"
MODE = "coastline"
FILE_PATH = f"./data/output/{COUNTRY}/{MODE}"

# %%
def get_way_geometry_from_overpy(way_id):
    api = overpy.Overpass()
    query_message = \
    f"""
    way({way_id}); 
    out body;
    >;
    out skel qt;
    """
    result = api.query(query_message)
    for key, way in enumerate(result.ways):
        linestring_coords = []
        for node in way.nodes:
            linestring_coords.append(Point(node.lon, node.lat))
        lineStrings = LineString(linestring_coords)

    return lineStrings


# %% Get addtional way
way_id = [341831948, 1014234098, 419777872, 22480747, 1085734436, 488754881, 253306499, 787243277, 1009003835, 1009003836]
ways = [get_way_geometry_from_overpy(i) for i in way_id]
df = geopandas.GeoDataFrame(ways,columns=["geometry"])
df["POLYGON_ID"] = way_id
df["POLYGON_NAME"] = "UNKNOWN"
df["HOFN_TYPE"] = "2"
df["ROAD_LEVEL"] = "0"

df.to_file(f"{FILE_PATH}/additional_coastline.geojson", driver="GeoJSON", index=False)
# %%
original_coastline = geopandas.read_file(f"{FILE_PATH}/divide_coastline.geojson")
additional_coastline = geopandas.read_file(f"{FILE_PATH}/additional_coastline.geojson")
additional_coastline = geopandas.GeoDataFrame(LineUtils.merged_current_level_ways(additional_coastline))
concated = pandas.concat([original_coastline, additional_coastline])
# %%
result = LineUtils.merged_current_level_ways(concated)
geopandas.GeoDataFrame(result).to_file(f"{FILE_PATH}/coastline.geojson", driver="GeoJSON")
geopandas.GeoDataFrame(result).to_csv(f"{FILE_PATH}/coastline.tsv", index=False, sep="\t")
# %%
left_bottom_corner_index = concated[concated["POLYGON_ID"]==22492739].index[0]
left_top_corner_index = concated[concated["POLYGON_ID"] == 49431499].index[0]
right_top_corner_index = concated[concated["POLYGON_ID"] == 890560578].index[0]
right_bottom_corner_index = concated[concated["POLYGON_ID"] == 488754881].index[0]

# %%
concated.at[left_bottom_corner_index, "geometry"] = brute_force_merge(concated.at[left_bottom_corner_index, "geometry"],brute_force_merge(concated.at[203, "geometry"], concated.at[204, "geometry"]))
concated.at[left_top_corner_index, "geometry"] = brute_force_merge(concated.at[206, "geometry"],concated.at[left_top_corner_index, "geometry"])
concated.at[right_bottom_corner_index, "geometry"] = brute_force_merge(concated.at[205, "geometry"],concated.at[right_bottom_corner_index, "geometry"])
concated.at[right_top_corner_index, "geometry"] = brute_force_merge(concated.at[right_top_corner_index, "geometry"], concated.at[207, "geometry"] )
concated.to_file(f"{FILE_PATH}/coastline.geojson", driver="GeoJSON")
# %%
for i in reversed([0,1,2,3,4]):
    result.pop(i)
# %%
geopandas.GeoDataFrame(result).to_file(f"{FILE_PATH}/coastline.geojson", driver="GeoJSON")
geopandas.GeoDataFrame(result).to_csv(f"{FILE_PATH}/coastline.tsv", index=False, sep="\t")

# %%

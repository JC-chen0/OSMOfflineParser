import osmium
from shapely import wkt
import geopandas
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import polygonize

wktfab = osmium.geom.WKTFactory()


# %%
class WayWaterHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.way_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}
        self.err_way_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}
        
    def way(self, w):
        self.get_way_waters(w)

    def get_way_waters(self, w):
        water_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        water = w.tags.get("natural")
        if water == "water":
            way = wkt.loads(wktfab.create_linestring(w))
            try:
                polygon = list(polygonize(way))
                self.append_water_attribute(self.way_waters, water_id, name, polygon[0])
            except:
                self.append_water_attribute(self.err_way_waters, water_id, name, way)

    def append_water_attribute(self, waters: dict, water_id: str, water_name: str, water_geometry):
        waters["water_id"].append(water_id)
        waters["water_name"].append(water_name)
        waters["water_geometry"].append(water_geometry)


# %%
class RelationWaterHandler(osmium.SimpleHandler):
    def __init__(self):
        super.__init__()
        self.rel_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}

    def relation(self, r):
        self.get_relation_waters(r)

    def get_relation_waters(self, r):
        water_id = r.id
        name = r.tags.get("name") if r.tags.get("name") else "UNKNOWN"
        water = r.tags.get("natural")
        if water == "water":
            for member in r.members:
                polygon_id = member.ref
                polygon = wkt.loads(wktfab.create_linestring(r.member))

    def append_rel_water_attribute(self, water_id: str, water_name: str, water_geometry):
        self.rel_waters["water_id"].append(water_id)
        self.rel_waters["water_name"].append(water_name)
        self.rel_waters["water_geometry"].append(water_geometry)


# %%
class AreaWaterHandler(osmium.SimpleHandler):
    def __init__(self):
        super.__init__()
        self.area_waters = {"area_id": [], "id": [], "geometry": []}

# %%
water_handler = WayWaterHandler()
print(dir(super(WayWaterHandler)))

# %%
water_handler = WayWaterHandler()
water_handler.apply_file("..//..//data//input//taiwan-latest.osm.pbf", idx="flex_mem", locations=True)
# %%
water_handler = WayWaterHandler()
water_handler.apply_file("..//..//data//input//taiwan-latest.osm.pbf", idx="flex_mem", locations=True)

way_result = geopandas.GeoDataFrame(water_handler.way_waters, geometry="water_geometry")
err_way_result = geopandas.GeoDataFrame(water_handler.err_way_waters, geometry="water_geometry")

way_result.to_file("..//..//data//output//way_water_result.geojson", driver="GeoJSON", encoding="utf-8")
err_way_result.to_file("..//..//data//output//err_way_water_result.geojson", driver="GeoJSON", encoding="utf-8")
# %%
# 海岸線作業
# 1. 本島海岸線，海岸線每一百公里sep一段，取其中一段的way_id作為海岸線一百公里的ID(Rule1)
# 2. 離島要是發現有一些海岸線太短的，忽略他(Rule2)
# 3. 假設台灣的海岸線總共1411公里，理論上分成15段，最後一段11公里，但現在換成14段，最後一段特別延伸，不要讓最後一段特別短

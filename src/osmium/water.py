import osmium
from shapely import wkt
import geopandas
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import polygonize
from osmium.osm._osm import RelationMemberList
wktfab = osmium.geom.WKTFactory()
#%%
class WaterHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.way_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}
        self.err_way_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}
        self.rel_waters = {'water_id': [], 'water_name': [], 'water_geometry': []}
        self.areas = {'id':[], 'geometry':[]}
        
        self.w = NotImplemented
    def way(self, w):
        self.get_way_waters(w)
        self.w = w
    
    def get_way_waters(self, w):
        water_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        water = w.tags.get("natural")
        if water == "water":    
            way = wkt.loads(wktfab.create_linestring(w))
            try:
                polygon = list(polygonize(way))
                self.append_water_attribute(self.way_waters,water_id,name,polygon[0])
            except:
                self.append_water_attribute(self.err_way_waters,water_id,name,way)
            
    def append_water_attribute(self, waters:dict ,water_id:str, water_name:str, water_geometry):
        waters["water_id"].append(water_id)
        waters["water_name"].append(water_name)
        waters["water_geometry"].append(water_geometry)
    
    # def area(self,a):
    #     self.get_area(a)
    
    def append_area(self,area_id,geometry):
        self.areas.get("id").append(area_id)
        self.areas.get("geometry").append(geometry)
    
    # def get_area(self,a):
    #     if a.tags.get("natural") == "water":
    #         if a.is_multipolygon:
    #             try:
    #                 area_id = a.id
    #                 geometry = wkt.loads(wktfab.create_multipolygon(a))
    #                 self.append_area(area_id, geometry)
    #             except :
    #                 pass
    
    def relation(self, r):
        self.get_relation_waters(r)
    
    def get_relation_waters(self,r):
        water_id = r.id
        name = r.tags.get("name") if r.tags.get("name") else "UNKNOWN"
        water = r.tags.get("natural")
        if water == "water":
            # polygon = wkt.loads(wktfab.create_multipolygon(r.members))
            for member in r.members:
                polygon_id = member.ref
                polygon = wkt.loads(wktfab.create_linestring(r.member))
                print(polygon)
                
            
    def append_rel_water_attribute(self, water_id:str, water_name:str, water_geometry):
        self.rel_waters["water_id"].append(water_id)
        self.rel_waters["water_name"].append(water_name)
        self.rel_waters["water_geometry"].append(water_geometry)

#%%
water_handler = WaterHandler()
water_handler.apply_file("..//..//data//input//taiwan-latest.osm.pbf",idx="flex_mem", locations= True)
#%%
area_result= geopandas.GeoDataFrame(water_handler.areas, geometry="geometry")
area_result.to_file("..//..//data//output//area_water_result.geojson", driver="GeoJSON", encoding="utf-8")
#%%
way_result = geopandas.GeoDataFrame(water_handler.way_waters, geometry="water_geometry")
err_way_result = geopandas.GeoDataFrame(water_handler.err_way_waters, geometry="water_geometry")
way_result.to_file("..//..//data//output//way_water_result.geojson", driver="GeoJSON", encoding="utf-8")
err_way_result.to_file("..//..//data//output//err_way_water_result.geojson",driver= "GeoJSON",encoding="utf-8")
# %%
print(way_result)
# print(rel_result)
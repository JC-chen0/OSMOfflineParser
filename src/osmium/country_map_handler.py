import osmium
import shapely
wktfab = osmium.geom.WKTFactory()
class CountryHandler(osmium.SimpleHandler):
    country_name = "Taiwan"
    country_relation_id = 449220

    def __init__(self):
        super().__init__()
        self.taiwan_geometry_dict = {'id': [], 'geometry': []}

    def relation(self, r):
        self.get_taiwan_relation(r)

    def get_taiwan_relation(self, relation):
        rel_id = relation.id
        if rel_id == CountryHandler.country_relation_id:
            w



    def set_taiwan_geometry_dict(self, rel_id, geometry):
        self.taiwan_geometry_dict.get("id").append(rel_id)
        self.taiwan_geometry_dict.get("geometry").append(geometry)


# %%
taiwan_handler = CountryHandler()
file_path = "data\\input\\taiwan-latest.osm.pbf"
taiwan_handler.apply_file(file_path, idx="flex_mem", locations=True)

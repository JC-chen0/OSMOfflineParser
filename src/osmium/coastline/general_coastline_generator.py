from copy import deepcopy
from typing import List, Dict

import osmium

wktfab = osmium.geom.WKTFactory()

from shapely import wkt
import geopandas
import time



# %%
class CoastlineHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.coastlines = {'id': [], 'name': [], 'boundary': [], 'geometry': []}
        self.ignore_list = {'id': [], 'name': [], 'boundary': [], 'geometry': []}

    def way(self, w):
        self.get_coastlines(w)


    # boundary and name if not exists, the coastline will be ignorable
    def get_coastlines(self, w):
        coastline_id = w.id
        name = w.tags.get("name") if w.tags.get("name") else "UNKNOWN"
        boundary = w.tags.get("boundary") if w.tags.get("boundary") else "UNKNOWN"
        natural = w.tags.get("natural")
        if natural == "coastline":
            coastline = wkt.loads(wktfab.create_linestring(w))
            try:
                self.append_coastline_attribute(self.coastlines, coastline_id, name, boundary, coastline)
            except Exception as e:
                pass

    def append_coastline_attribute(self, attributes: dict, coastline_id: str, name: str, boundary: str,
                                   geometry):
        attributes["id"].append(coastline_id)
        attributes["name"].append(name)
        attributes["boundary"].append(boundary)
        attributes["geometry"].append(geometry)


# %% Get data from handler
start_time = time.time()
coastline_handler = CoastlineHandler()
coastline_handler.apply_file("data/input/taiwan-latest.osm.pbf", idx="flex_mem", locations=True)
# %% Gen data to output file
coastline_df = geopandas.GeoDataFrame(coastline_handler.coastlines, geometry="geometry")
coastline_df.to_file("data/input/general_coastline_taiwan.geojson", driver="GeoJSON")
print(f"Loading data process takes {time.time() - start_time} seconds")

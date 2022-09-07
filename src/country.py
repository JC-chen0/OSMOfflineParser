from copy import copy

import geopandas
import osmium
import pandas
from shapely import wkt
from osmium.geom import WKTFactory


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


class CountryRelationHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkt_factory = WKTFactory()
        self.country_id = []
        self.country_relation_id = 536780

    def relation(self, r):
        if r.id == self.country_relation_id:
            for member in r.members:
                print(member)
                if member.type == "w":
                    self.country_id.append(member.ref * 2)
                if member.type == "r":
                    self.country_id.append((member.ref * 2) + 1)


class CountryAreaHandler(osmium.SimpleHandler):
    country_id = []

    def __init__(self):
        super().__init__()
        self.wkt_factory = WKTFactory()
        self.areas = []

    def area(self, a):
        self.get_area(a)

    # way 1
    def get_area(self, a):
        try:
            if a.id in self.country_id:
                poly_str = wkt.loads(self.wkt_factory.create_multipolygon(a))
                area = {"POLYGON_ID": a.id, "POLYGON_STR": poly_str}
                area = merge_two_dicts(area, dict())
                self.areas.append(area)
        except:
            pass


# Get relation and sub area id.
country_relation_handler = CountryRelationHandler()
country_relation_handler.apply_file("data/input/country/malaysia-singapore-brunei-latest.osm.pbf", idx="flex_mem",
                                    locations=True)

# Get Area
CountryAreaHandler.country_id = country_relation_handler.country_id
country_area_handler = CountryAreaHandler()
country_area_handler.apply_file("data/input/country/malaysia-singapore-brunei-latest.osm.pbf", idx="flex_mem",
                                locations=True)

# %%
df = geopandas.GeoDataFrame(country_area_handler.areas, geometry="POLYGON_STR")
df.to_csv("data/output/country/taiwan.tsv", sep="\t")

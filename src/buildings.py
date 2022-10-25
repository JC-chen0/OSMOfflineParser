import logging
import time
from typing import Dict

import geopandas
import osmium
import pandas
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import polygonize
from src.enum.tag import Tag
from src.util.limit_area import LimitAreaUtils
from src.util.merging_utils import RingUtils

wktfab = osmium.geom.WKTFactory()


class BuildingHandler(osmium.SimpleHandler):

    def __init__(self, tags):
        super().__init__()
        self.way_buildings = []
        self.way_dict = {}
        self.relation_dict = {}
        self.tags = tags

    def area(self, area):
        try:
            if area.tags.get("building") or area.tags.get("type") == "building":
                ring_id = area.orig_id()
                ring_name = area.tags.get("name") if area.tags.get("name") else "UNKNOWN"  # create new string object
                ring_geometry = wkt.loads(wktfab.create_multipolygon(area))
                ring_height = area.tags.get("height") if area.tags.get("height") else "UNKNOWN"
                ring_level = area.tags.get("building:levels") if area.tags.get("building:levels") else "UNKNOWN"
                if area.from_way():
                    # All area from way is one polygon (len(geometry) == 1)
                    ring_geometry = list(ring_geometry)[0]  # Extract polygon from multipolygon
                    building = Building(polygon_id=ring_id, polygon_name=ring_name, geometry=ring_geometry, height=ring_height, level=ring_level)
                    self.way_buildings.append(building)
        except:
            pass

    def relation(self, relation):
        if relation.tags.get("building") or relation.tags.get("type") == "building":
            for member in relation.members:
                if not self.relation_dict.get(relation.id, False):
                    self.relation_dict[relation.id] = []
                self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name") if way.tags.get("name") else "UNKNOWN", "geometry": way_geometry,
                                 "height": way.tags.get("height") if way.tags.get("height") else "UNKNOWN",
                                 "level": way.tags.get("building:levels") if way.tags.get("building:levels") else "UNKNOWN"}


class Building:

    def __init__(self, polygon_id, polygon_name, geometry, height, level, relation_id=None, role=None):
        self.polygon_id = polygon_id
        self.polygon_name = polygon_name
        self.height = height
        self.level = level
        self.geometry = geometry
        self.relation_id = relation_id
        self.role = role


def get_relation_member_data_building(relation_dict: Dict, way_dict: Dict, tags: list) -> Dict:
    ring_rel_members_dict = {"relation_id": [], "way_id": [], "name": [], "geometry": [], "role": [], "type": [], "height": [], "level": []}

    for relation_id, members in relation_dict.items():
        for member in members:
            if member.get("role") not in tags:
                continue
            way_id = member.get("id")
            way = way_dict.get(way_id, False)
            if way is False:
                continue
            name = way.get("name")
            role = member.get("role")
            if role == '':
                role = "outer"
            if way:
                ring_rel_members_dict.get("relation_id").append(relation_id)
                ring_rel_members_dict.get("way_id").append(way_id)
                ring_rel_members_dict.get("name").append(way.get("name"))
                ring_rel_members_dict.get("geometry").append(way.get("geometry"))
                ring_rel_members_dict.get("role").append(role)
                ring_rel_members_dict.get("type").append(member.get("type"))
                ring_rel_members_dict.get("height").append(way.get("height"))
                ring_rel_members_dict.get("level").append(way.get("level"))
            else:
                pass
    return ring_rel_members_dict


# %%
def main(input_path, output_path, nation, limit_relation_id, DEBUGGING=False, ALL_OFFLINE=True):
    start_time = time.time()
    logging.info("[1/2] Getting data from .osm.pbf . ")
    if ALL_OFFLINE:
        limit_area = LimitAreaUtils.get_limit_relation_geom(input_path, limit_relation_id)
    else:
        limit_area = LimitAreaUtils.get_relation_polygon_with_overpy(limit_relation_id)

    building_handler = BuildingHandler(Tag["building"].value)
    building_handler.apply_file(input_path, idx="flex_mem", locations=True)
    relation_dict = building_handler.relation_dict
    way_dict = building_handler.way_dict

    # %%
    way_buildings = building_handler.way_buildings
    way_buildings_in_limit_area = []
    for way_building in way_buildings:
        if way_building.geometry.within(limit_area):
            way_buildings_in_limit_area.append(Building(way_building.polygon_id, way_building.polygon_name, way_building.geometry, way_building.height, way_building.level))
    header = ["POLYGON_ID", "POLYGON_NAME", "geometry", "HEIGHT", "LEVEL"]
    gdf = geopandas.GeoDataFrame([[building.polygon_id, building.polygon_name, building.geometry, building.height, building.level] for building in way_buildings_in_limit_area], columns=header)
    gdf.to_file(f"{output_path}/way_buildings.geojson", driver="GeoJSON") if DEBUGGING else None
    # %%
    logging.info("[2/2] Extract inner from outer and get all the part and outline as polygons.")
    relation_member_dict = get_relation_member_data_building(relation_dict=relation_dict, way_dict=way_dict, tags=["outer", "inner", "", "outline", "part"])
    relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict)
    relation_member_data = LimitAreaUtils.prepare_data(relation_member_data, limit_area.wkt)
    relation_member_dict = relation_member_data.to_dict("index")

    # %%
    temp = dict()
    for member in relation_member_dict.values():
        relation_id = member.get("relation_id")

        member.pop("relation_id")
        geometry = Polygon(member.get("geometry"))
        building = Building(relation_id=relation_id, polygon_id=member.get("way_id"), polygon_name=member.get("name"), geometry=geometry, role=member.get("role"), height=member.get("height"), level=member.get("level"))
        if not temp.get(relation_id, 0):
            temp[relation_id] = {"outer": [], "inner": [], "other": []}

        if member.get("role") == "inner":
            temp.get(relation_id).get("inner").append(building)
        elif member.get("role") == "outer":
            temp.get(relation_id).get("outer").append(building)
        else:  # ONLY for debug purpose.
            temp.get(relation_id).get("other").append(building)
    relation_member_dict = temp

    logging.info("Group data completed, start to extract.")
    # %%
    relation_result = geopandas.GeoDataFrame(columns=header)
    for relation_id, relation in relation_member_dict.items():

        outers = relation.get("outer")
        inners = relation.get("inner")
        others = relation.get("other")

        outers_gdf = geopandas.GeoDataFrame([[outer.polygon_id, outer.polygon_name, outer.geometry, outer.height, outer.level] for outer in outers], columns=header)
        inners_gdf = geopandas.GeoDataFrame([[inner.polygon_id, inner.polygon_name, inner.geometry, inner.height, inner.level] for inner in inners], columns=header)
        others_gdf = geopandas.GeoDataFrame([[other.polygon_id, other.polygon_name, other.geometry, other.height, other.level] for other in others], columns=header)
        if outers and inners:
            outers_gdf_clipped = geopandas.overlay(outers_gdf, inners_gdf, how="difference")
            relation_result = pandas.concat([relation_result, outers_gdf_clipped])
        if others:
            relation_result = pandas.concat([relation_result, others_gdf])
    logging.info("Extraction completed, start to output file.")
    # %%
    relation_result.to_file(f"{output_path}/relation_buildings.geojson", driver="GeoJSON") if DEBUGGING else None

    # %%
    result = pandas.concat([gdf, relation_result])
    result.to_file(f"{output_path}/buildings.geojson", driver="GeoJSON") if DEBUGGING else result.to_csv(f"{output_path}/buildings.tsv", sep="\t")
    logging.info("Program completed.")

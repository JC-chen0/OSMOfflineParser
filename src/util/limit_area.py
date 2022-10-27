import logging
import math
from typing import Dict

import geopandas
import overpy
from geopandas import GeoDataFrame
from shapely import wkt
import osmium
from shapely.geometry import Polygon, MultiPolygon, Point, LineString
from shapely.ops import linemerge, unary_union, polygonize
from src.util.merging_utils import RingUtils

wktfab = osmium.geom.WKTFactory()


class LimitRelationAreaHanlder(osmium.SimpleHandler):
    def __init__(self, relation_id):
        super().__init__()
        self.way_dict: Dict[Dict] = dict()
        self.relation_id = relation_id
        self.relation_dict = dict()

    def relation(self, relation):
        if relation.id == int(self.relation_id):
            for member in relation.members:
                if member.ref in self.way_dict and member.role == "outer" and member.type == "w":

                    if not self.relation_dict.get(relation.id, False):
                        self.relation_dict[relation.id] = []
                    self.relation_dict[relation.id].append({"id": member.ref, "role": member.role, "type": member.type})

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"id": way.id, "name": way.tags.get("name"), "geometry": way_geometry}


class LimitAreaUtils:
    @staticmethod
    def get_limit_relation_geom(filepath, relation_id):
        handler = LimitRelationAreaHanlder(relation_id)
        handler.apply_file(filepath, idx="flex_mem", locations=True)
        way_dict = handler.way_dict
        relation_dict = handler.relation_dict
        relation_member_dict = RingUtils.get_relation_member_data(relation_dict=relation_dict, way_dict=way_dict, tags=["outer", "inner", ""])
        relation_member_data: geopandas.GeoDataFrame = geopandas.GeoDataFrame(relation_member_dict)
        relation_member_dict = relation_member_data.to_dict("index")
        relation_member_dict = RingUtils.restructure(relation_member_dict)
        relation_result = []
        polygon_id_used_table = []
        for relation_id, relation in relation_member_dict.items():
            logging.debug(f"Relation: {relation_id} doing merge.")

            outers = relation.get("outer")
            if outers:
                outers = RingUtils.get_merged_rings(outers, polygon_id_used_table, "water")
                relation_member_dict[relation_id] = outers
                for outer in outers:
                    relation_result.append(outer)

        geom = MultiPolygon([Polygon(i.get("geometry")) for i in relation_result])

        logging.debug("Get limit relation area geometry completed.")
        return geom

    @staticmethod
    def get_relation_polygon_with_overpy(rel_id: str) -> MultiPolygon:
        api = overpy.Overpass()
        query_msg = f"""
        [out:json][timeout:25];
        rel({rel_id});
        out body;
        >;
        out skel qt; 
        """
        result = api.query(query_msg)
        lineStrings = []
        for key, way in enumerate(result.ways):
            linestring_coords = []
            for node in way.nodes:
                linestring_coords.append(Point(node.lon, node.lat))
            lineStrings.append(LineString(linestring_coords))

        merged = linemerge([*lineStrings])
        borders = unary_union(merged)
        polygons = MultiPolygon(list(polygonize(borders)))
        return polygons

    @staticmethod
    def prepare_data(data_df: GeoDataFrame, intersection_polygon_wkt: str) -> GeoDataFrame:
        intersects_geom = wkt.loads(intersection_polygon_wkt)
        if intersects_geom.type == "LineString":
            intersects_geom = intersects_geom.buffer(1/6371000/math.pi*180)
        intersects_series = geopandas.GeoSeries(intersects_geom)
        intersects_indices = list(data_df.sindex.query_bulk(intersects_series, predicate="intersects")[1])
        data_df = data_df.iloc[intersects_indices]
        return data_df

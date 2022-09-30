import logging
from typing import Dict

import overpy
from shapely import wkt
import osmium
from shapely.geometry import Polygon, MultiPolygon, Point, LineString
from shapely.ops import linemerge, unary_union, polygonize

wktfab = osmium.geom.WKTFactory()


class LimitRelationAreaHanlder(osmium.SimpleHandler):
    def __init__(self, relation_id):
        super().__init__()
        self.way_dict: Dict[Dict] = dict()
        self.relation_id = relation_id
        self.relation_outer_ids = []

    def relation(self, relation):
        if relation.id == int(self.relation_id):
            for member in relation.members:
                if member.role == "outer" and member.type == "w":
                    self.relation_outer_ids.append(member.ref)

    def way(self, way):
        way_geometry = wkt.loads(wktfab.create_linestring(way))
        self.way_dict[way.id] = {"ID": way.id, "NAME": way.tags.get("name"), "GEOMETRY": way_geometry}


def get_limit_relation_geom(filepath, relation_id):
    handler = LimitRelationAreaHanlder(relation_id)
    handler.apply_file(filepath, idx="flex_mem", locations=True)
    way_dict = handler.way_dict
    relation_outers_id = handler.relation_outer_ids
    geom = MultiPolygon([Polygon(way_dict[outer_id]) for outer_id in relation_outers_id])
    logging.debug("Get limit relation area geometry completed.")
    return geom


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

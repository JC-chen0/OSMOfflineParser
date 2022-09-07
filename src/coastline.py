import geopandas
import time
import math
import osmium
import overpy
from copy import deepcopy
from typing import List, Dict

import pandas
from geopandas import GeoDataFrame
from shapely.ops import polygonize, linemerge, unary_union
from shapely import wkt
from shapely.geometry import Point, LineString, MultiPolygon


wkt_factory = osmium.geom.WKTFactory()


class CoastlineHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.coastlines = {'id': [], 'geometry': []}

    def way(self, w):
        self.get_coastlines(w)

    def get_coastlines(self, w):
        coastline_id = w.id
        natural = w.tags.get("natural")
        if natural == "coastline":
            coastline = wkt.loads(wkt_factory.create_linestring(w))
            try:
                self.append_coastline_attribute(self.coastlines, coastline_id, coastline)
            except Exception as e:
                pass

    def append_coastline_attribute(self, attributes: dict, coastline_id: str, geometry):
        attributes["id"].append(coastline_id)
        attributes["geometry"].append(geometry)


coastline_handler = CoastlineHandler()
coastline_handler.apply_file("data\\input\\country\\taiwan-latest.osm.pbf", idx="flex_mem", locations=True)
coastline_df = geopandas.GeoDataFrame(coastline_handler.coastlines, geometry="geometry")
coastline_df.to_file("data\\output\\coastline\\unmerged_coastline.geojson", driver="GeoJSON")


# Merging coastline
# intersect with taiwan
def prepare_data(file_path: str, intersection_polygon_wkt: str) -> GeoDataFrame:
    coastline_df = geopandas.read_file(file_path)
    polygon = wkt.loads(intersection_polygon_wkt)
    coastline_df["in_polygon"] = coastline_df.apply(lambda x: bool(polygon.intersection(x.geometry)), axis=1)
    coastline_df = coastline_df[coastline_df["in_polygon"]]
    del coastline_df["in_polygon"]
    # coastline_df.reset_index(inplace=True, drop=True)
    return coastline_df


def lonlat_length_in_km(geom):
    return geom.length * 6371 * math.pi / 180


def reverse_linestring_coords(geometry) -> LineString:
    geometry.coords = list(geometry.coords)[::-1]


def is_continuous(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return head == compare_tail or tail == compare_head


def is_reverse_needed(line1, line2):
    head, tail = line1.coords[0], line1.coords[-1]
    compare_head, compare_tail = line2.coords[0], line2.coords[-1]
    return (head == compare_head and tail != compare_tail) or (tail == compare_tail and head != compare_head)


def get_merge_coastline(coastline: GeoDataFrame) -> Dict:
    coastline_dict = coastline.set_index(coastline["id"]).to_dict('index')
    coastline_merge_dict = deepcopy(coastline_dict)
    start_time = time.time()
    processed_list = []
    for id, coastline in coastline_dict.items():
        if id in processed_list:
            continue
        geometry = coastline["geometry"]
        del_id = NotImplemented
        print("=================================")
        print(f"{id} start merging.")
        merging = True
        while merging:
            for sub_id, sub_coastline in coastline_merge_dict.items():
                compare_geometry = sub_coastline["geometry"]

                if sub_id == list(coastline_merge_dict.keys())[-1]:
                    print(f"{id} merge process completed, start another round.")
                    merging = False
                    break

                if id == sub_id:
                    continue

                if is_reverse_needed(geometry, compare_geometry):
                    compare_geometry = reverse_linestring_coords(compare_geometry.wkt)
                elif is_continuous(geometry, compare_geometry):
                    merge_linestring = linemerge([compare_geometry, geometry])
                    geometry = merge_linestring
                    coastline_merge_dict.get(id)["geometry"] = geometry

                    # remove merged id
                    del_id = sub_id
                    coastline_merge_dict.pop(del_id)

                    processed_list.append(sub_id)
                    print(f"{id} merge with {sub_id}, break, {sub_id} will be removed.")
                    break  # break inner for loop to start another round of merging
    print("=================================")
    print(f"Merging process complete, taking: {time.time() - start_time} seconds")
    return coastline_merge_dict


def merge_and_divided_by_threshold(mainland_coastline_dict, length_threshold):
    compare_coastline_dict = deepcopy(mainland_coastline_dict)
    reach_length_limit_list = []
    start_id = next(iter(compare_coastline_dict))
    start_line = compare_coastline_dict.get(start_id).get("geometry")
    merging = True
    last_segment = False
    count = 0
    while merging:
        for compare_poly_id, compare_poly_dict in compare_coastline_dict.items():
            compare_geometry = compare_poly_dict["geometry"]

            if compare_poly_id in reach_length_limit_list and not last_segment:
                if len(compare_coastline_dict.keys()) - len(reach_length_limit_list) == 1:
                    last_segment = True
                    break
                continue

            if start_id == compare_poly_id:
                continue

            if is_reverse_needed(start_line, compare_geometry):
                compare_geometry = reverse_linestring_coords(compare_geometry)
            elif is_continuous(start_line, compare_geometry):
                merge_linestring = linemerge([start_line, compare_geometry])
                if merge_linestring.length * 6371 * math.pi / 180 >= length_threshold and not last_segment:
                    reach_length_limit_list.append(start_id)
                    compare_coastline_dict.get(start_id)["geometry"] = start_line
                    # Restart looping
                    print(f"{start_id} reach length limit. Next start line will use {compare_poly_id}")
                    start_id = compare_poly_id
                    start_line = compare_geometry
                    break
                elif last_segment:
                    compare_coastline_dict.get(start_id)["geometry"] = merge_linestring
                    merging = False
                start_line = merge_linestring
                # remove merged id
                compare_coastline_dict.pop(compare_poly_id)
                print(f"{start_id} merge with {compare_poly_id}, break, {compare_poly_id} will be removed.")
                break
    return compare_coastline_dict


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


def get_relation_polygon(rel_id: str) -> MultiPolygon:
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


#  Merge all coastline
taiwan_territorial_geom = get_relation_polygon("449220")
# taiwan_territorial_geom = wkt.loads(
#     "MULTIPOLYGON (((114.3986111 10.4402778, 114.4269444 10.3988889, 114.4233333 10.3491667, 114.3863889 10.3297222, 114.3652778 10.3269444, 114.3361111 10.3266667, 114.2880556 10.3469444, 114.2938889 10.4152778, 114.3288889 10.4363889, 114.3511111 10.4397222, 114.3986111 10.4402778)), ((116.6855033 20.8547596, 116.7513728 20.8747063, 116.8205709 20.872399, 116.8847512 20.8481147, 116.9361727 20.8047732, 116.9686331 20.7475883, 116.9782173 20.6834462, 116.9637692 20.6200797, 116.9270316 20.5651373, 116.8724354 20.5252581, 116.8065659 20.5052653, 116.7373678 20.5075783, 116.6731874 20.5319171, 116.621766 20.5753367, 116.5893056 20.6325865, 116.5797214 20.6967501, 116.5941695 20.7600846, 116.6309071 20.8149565, 116.6855033 20.8547596)), ((118.1316667 24.3822222, 118.1366824 24.3913021, 118.1469442 24.4100007, 118.1969442 24.4375007, 118.2061109 24.4522229, 118.2316665 24.4780563, 118.2594172 24.4953791, 118.2752776 24.5052785, 118.3308331 24.5088896, 118.3844442 24.5319451, 118.4052776 24.5427785, 118.4494442 24.5286118, 118.4613887 24.518334, 118.5227776 24.4952785, 118.5294442 24.4702785, 118.5461109 24.4269451, 118.5094442 24.3636118, 118.4644442 24.3222229, 118.3866665 24.3413896, 118.319722 24.2941674, 118.2761109 24.3066674, 118.2249998 24.3122229, 118.1552778 24.3602778, 118.1316667 24.3822222)), ((118.1380491 24.3746216, 118.1380786 24.3746241, 118.1380839 24.3746094, 118.1380544 24.3745996, 118.1380383 24.374585, 118.1380142 24.374585, 118.1379902 24.3745918, 118.1380142 24.3746119, 118.1380491 24.3746216)), ((118.1381269 24.3747145, 118.1381619 24.3747189, 118.1381671 24.3746925, 118.1381377 24.3746749, 118.1381109 24.3746505, 118.1381002 24.3746798, 118.1381269 24.3747145)), ((118.1383119 24.3748977, 118.1383387 24.3748831, 118.1383468 24.3748488, 118.1383334 24.3748269, 118.1383307 24.3748, 118.1383012 24.3747853, 118.1382797 24.374756, 118.138269 24.3747365, 118.1382234 24.3747218, 118.1381966 24.374712, 118.1381859 24.3747365, 118.1381914 24.374758, 118.138218 24.3747707, 118.138237 24.3747971, 118.1382611 24.3748215, 118.1382368 24.374844, 118.1382611 24.3748679, 118.1382745 24.3748728, 118.1383119 24.3748977)), ((118.1385694 24.3746534, 118.138591 24.3746529, 118.1385936 24.374629, 118.1385696 24.3746138, 118.1385345 24.3746045, 118.1385319 24.3746192, 118.1385426 24.3746412, 118.1385694 24.3746534)), ((118.1386472 24.3749319, 118.1386821 24.3749148, 118.1387144 24.3749265, 118.1387411 24.3749441, 118.1387654 24.374951, 118.1387491 24.3749246, 118.1387277 24.3749075, 118.1386955 24.3748831, 118.1386955 24.3748586, 118.1386928 24.3748342, 118.1386901 24.374822, 118.1387251 24.3748313, 118.1387493 24.3748337, 118.138784 24.3748171, 118.1388029 24.3748019, 118.1388323 24.3748122, 118.1388671 24.3747853, 118.1388832 24.3747756, 118.1389369 24.3747585, 118.1388993 24.3747242, 118.1388591 24.3747096, 118.1388834 24.3746822, 118.1388725 24.3746558, 118.1388296 24.3746412, 118.1387974 24.3746387, 118.1387572 24.3746339, 118.138725 24.3746339, 118.1386848 24.3746339, 118.1386472 24.374651, 118.1386284 24.3746705, 118.138615 24.3746974, 118.138615 24.374734, 118.1385936 24.374712, 118.1385694 24.3746827, 118.1385399 24.3746534, 118.1385104 24.3746412, 118.1384809 24.3746265, 118.1384755 24.3746045, 118.1384863 24.3745801, 118.1384621 24.3745532, 118.1384434 24.3745239, 118.1383897 24.3745361, 118.1383575 24.3745312, 118.1383334 24.3745068, 118.1383092 24.3744897, 118.1382906 24.3744746, 118.1382663 24.3744702, 118.1382395 24.3744677, 118.1382154 24.3744677, 118.1381939 24.3744726, 118.1381698 24.3744946, 118.1381699 24.3745381, 118.1381564 24.374563, 118.1381832 24.3745923, 118.1381994 24.3746114, 118.1382209 24.3746407, 118.138245 24.3746627, 118.138269 24.3747071, 118.1382879 24.3747213, 118.1383228 24.3747506, 118.1383711 24.37478, 118.1384004 24.3748073, 118.1384192 24.3748513, 118.1384381 24.3748679, 118.1384621 24.3748831, 118.1384997 24.3748977, 118.1385213 24.374907, 118.1385506 24.3749124, 118.138583 24.3749192, 118.1386204 24.3749246, 118.1386472 24.3749319)), ((118.2394443 24.2077775, 118.2641666 24.2066664, 118.2916666 24.1788886, 118.2941666 24.130833, 118.2524999 24.1152775, 118.2327777 24.1152775, 118.2133332 24.1205553, 118.1888888 24.1422219, 118.1888888 24.1730553, 118.2069443 24.2049997, 118.2394443 24.2077775)), ((119.4616667 25.0552778, 119.4855556 25.0408333, 119.5113889 25.0402778, 119.5319444 25.0041667, 119.5377778 24.9763889, 119.5286111 24.9538889, 119.5097222 24.9330556, 119.4922222 24.9263889, 119.4755556 24.9238889, 119.4516667 24.9263889, 119.4172222 24.9438889, 119.4019444 24.9566667, 119.3911111 24.9713889, 119.3858333 24.9894444, 119.3858333 25.0316667, 119.4308333 25.0502778, 119.4616667 25.0552778)), ((119.9388889 26.0377778, 120.0213889 26.0125, 120.0369444 26.0052778, 120.0502778 25.9666667, 120.0330556 25.9316667, 120.0041667 25.9025, 119.9622222 25.8863889, 119.9213889 25.9005556, 119.8777778 25.9427778, 119.8591667 25.9766667, 119.8763889 26.0136111, 119.9388889 26.0377778)), ((119.9411111 26.2955554, 119.9475 26.3066665, 119.9711111 26.3169442, 119.9827778 26.324722, 120.0302778 26.3255554, 120.0755556 26.2552776, 120.0752778 26.2302776, 120.0769444 26.212222, 120.0575 26.1727776, 120.0219444 26.1277776, 119.9938889 26.1105554, 119.9741667 26.1024998, 119.9275 26.0836109, 119.8666667 26.1077776, 119.8488889 26.144722, 119.8561111 26.1766665, 119.8752778 26.2149998, 119.9263889 26.2608331, 119.9411111 26.2955554)), ((119.9578607 26.0811846, 119.9579599 26.0811244, 119.9579653 26.0810329, 119.9578848 26.0809461, 119.9577722 26.0809413, 119.9576971 26.0809967, 119.9576971 26.0811124, 119.9578607 26.0811846)), ((119.9654116 26.0856463, 119.9653538 26.0852871, 119.9652271 26.085052, 119.9650454 26.0848506, 119.9648347 26.0847015, 119.9647095 26.0846701, 119.964647 26.0846285, 119.9645808 26.084631, 119.9644546 26.0845302, 119.9641602 26.084419, 119.9639186 26.0843648, 119.9637293 26.0843506, 119.9635578 26.084351, 119.9633378 26.0843766, 119.9631232 26.0842462, 119.9629078 26.0842106, 119.9627646 26.0840573, 119.9621483 26.083863, 119.9617622 26.0838478, 119.9615367 26.0839666, 119.9610661 26.0841168, 119.9606938 26.0844318, 119.9604929 26.0849673, 119.9604959 26.0850638, 119.9605814 26.0853505, 119.9604845 26.0855026, 119.960489 26.0855789, 119.9604396 26.0856033, 119.9604057 26.0857298, 119.9603745 26.0857645, 119.9603745 26.0858356, 119.9603286 26.0860154, 119.9603344 26.0861216, 119.9604479 26.0862225, 119.9604864 26.0863474, 119.960429 26.0864704, 119.9601759 26.0867848, 119.9601144 26.0870933, 119.9601594 26.0873752, 119.9604227 26.0878112, 119.9605122 26.0878926, 119.961123 26.088309, 119.9615636 26.0883582, 119.9620565 26.0882409, 119.9621005 26.0881852, 119.9622472 26.0881901, 119.9625722 26.0880856, 119.9626143 26.0880106, 119.9628196 26.0878522, 119.9627883 26.0876542, 119.9630448 26.0871422, 119.9634772 26.0870393, 119.9636908 26.0866032, 119.9638874 26.0866161, 119.9641441 26.0865537, 119.9641857 26.0865779, 119.9644077 26.0865467, 119.9645443 26.0865519, 119.9649455 26.0864115, 119.9654085 26.0857021, 119.9654116 26.0856463)), ((119.9659717 26.0918709, 119.9659395 26.0916782, 119.9656981 26.0916541, 119.9655801 26.0913216, 119.9653816 26.091153, 119.9652636 26.0911675, 119.9651992 26.0916782, 119.96521 26.0919094, 119.9656123 26.0921214, 119.9659717 26.0918709)), ((120.0770085 26.386327, 120.07951 26.388001, 120.0818176 26.3890355, 120.0835752 26.3895711, 120.0858912 26.3896834, 120.0871254 26.3896556, 120.0889787 26.3894371, 120.0915903 26.3891604, 120.0931053 26.3867708, 120.0941108 26.3846259, 120.0935992 26.3818854, 120.0905711 26.3796073, 120.0877561 26.3781022, 120.0845638 26.3775283, 120.0811481 26.3765344, 120.0782294 26.378343, 120.0767893 26.3799946, 120.0760225 26.3811242, 120.0757634 26.3834968, 120.0770085 26.386327)), ((120.2023065 26.2494006, 120.2023816 26.2491793, 120.2021885 26.2488907, 120.2020383 26.2492274, 120.2023065 26.2494006)), ((120.2024674 26.2516138, 120.202285 26.2518062, 120.2026498 26.2517774, 120.2024674 26.2516138)), ((120.2136109 26.4041667, 120.2666665 26.3972222, 120.289722 26.3613889, 120.289722 26.3252778, 120.2666665 26.3005556, 120.2466665 26.285, 120.2205554 26.28, 120.1777776 26.2841667, 120.1538887 26.32, 120.1538887 26.3516667, 120.1674998 26.3702778, 120.1805554 26.3822222, 120.2136109 26.4041667)), ((120.403727 26.1560059, 120.4036653 26.1561095, 120.4034909 26.1562154, 120.4031933 26.1562346, 120.4030109 26.1564176, 120.4029786 26.1565308, 120.4028446 26.1566921, 120.4027935 26.156851, 120.4026273 26.1570364, 120.4026004 26.157176, 120.4025066 26.1571736, 120.4024556 26.1572675, 120.4022732 26.1574096, 120.4022464 26.1575492, 120.4023591 26.1576527, 120.4025401 26.1577755, 120.4027265 26.1580716, 120.4027667 26.1583051, 120.4027534 26.1584761, 120.4028123 26.1587337, 120.4029277 26.1587481, 120.4031476 26.1585411, 120.4032684 26.1583413, 120.4032147 26.1581751, 120.4030431 26.1581101, 120.4029867 26.1580572, 120.4029974 26.1579416, 120.4028103 26.1578285, 120.4027651 26.1577286, 120.4029409 26.1576016, 120.4031973 26.1576479, 120.4033435 26.1576094, 120.4034419 26.1573816, 120.4036583 26.1572742, 120.403949 26.1573529, 120.4042406 26.1573938, 120.4045571 26.1574707, 120.404757 26.1574409, 120.4049179 26.1573566, 120.4049742 26.157229, 120.4050708 26.1570556, 120.4050975 26.1569907, 120.4051244 26.1568149, 120.4051244 26.1566801, 120.4050815 26.1564971, 120.4050547 26.1563936, 120.4053095 26.1563454, 120.4054141 26.1563623, 120.4056663 26.1567089, 120.4057923 26.1567427, 120.4059666 26.1565982, 120.4062108 26.1565789, 120.4063019 26.1564706, 120.406369 26.156254, 120.4064011 26.15616, 120.4064119 26.1560349, 120.4063851 26.1559121, 120.4063153 26.1558711, 120.4063046 26.1557171, 120.4062 26.1556136, 120.4061866 26.1555245, 120.4061866 26.1554089, 120.4060739 26.1553054, 120.4058754 26.1552837, 120.4058111 26.1554281, 120.4055428 26.1555726, 120.4054678 26.1556761, 120.4054141 26.1557267, 120.4052826 26.1558254, 120.4051002 26.1559049, 120.4049635 26.1558134, 120.4047542 26.1558254, 120.4046174 26.1559072, 120.4045665 26.1557483, 120.4042768 26.1557195, 120.4041481 26.1557941, 120.4040113 26.1558206, 120.4038208 26.1559675, 120.403727 26.1560059)), ((120.4908333 26.4372222, 120.5363889 26.4252778, 120.5561111 26.4086111, 120.5758333 26.3875, 120.5775 26.3594444, 120.5683333 26.325, 120.5202778 26.3044444, 120.4616667 26.2872222, 120.4161111 26.3519444, 120.4130556 26.3905556, 120.4466667 26.4333333, 120.4908333 26.4372222)), ((122.1791 24.794, 122.1171 24.5539, 122.1127 24.5394, 122.0716 24.4206, 122.0713 24.4198, 121.7348 23.4531, 121.7391 22.6979, 121.8313 21.9731, 121.8226 21.8892, 121.8188 21.8779, 121.665 21.7443, 120.8789 21.5625, 120.656 21.6343, 120.191 22.1918, 119.2626 23.0518, 119.2368 23.0804, 119.1389 23.2117, 119.1011 23.313, 119.0963 23.3876, 119.0994 23.4351, 119.1002 23.4394, 119.1155 23.4848, 119.1196 23.4932, 119.1269 23.5069, 119.3489 23.8829, 119.3924 23.9341, 120.8846 25.1806, 120.919 25.2038, 120.9843 25.2394, 121.3907 25.4602, 121.9502 25.7988, 122.0552 25.8318, 122.0657 25.8326, 122.2789 25.7285, 122.2811 25.725, 122.3055 25.6579, 122.3283 25.5119, 122.3264 25.4458, 122.2243 24.9704, 122.2225 24.963, 122.1791 24.7943, 122.1791 24.794)))")

coastline = prepare_data(file_path="data\\output\\coastline\\unmerged_coastline.geojson",
                         intersection_polygon_wkt=taiwan_territorial_geom.wkt)
coastline_merge_dict = get_merge_coastline(coastline)
filter_small_island(coastline_merge_dict, area_threshold=40000)
df = geopandas.GeoDataFrame.from_dict(coastline_merge_dict, orient="index")
df["length"] = df.apply(lambda row: lonlat_length_in_km(row["geometry"]), axis=1)
df.to_file("data\\output\\coastline\\merged_coastline.geojson", driver="GeoJSON")
print("=================================")
print("Merging coastline process completed")

#  Merging taiwan mainland
merged_coastline: GeoDataFrame = geopandas.read_file("data\\output\\coastline\\merged_coastline.geojson")
mainland_coastline_wkt = list(merged_coastline.loc[merged_coastline["id"] == 9406157]["geometry"])[0].wkt
mainland_coastline = prepare_data(file_path="data\\output\\coastline\\unmerged_coastline.geojson",
                                  intersection_polygon_wkt=mainland_coastline_wkt)
mainland_coastline_dict = mainland_coastline.set_index(mainland_coastline["id"]).to_dict('index')
mainland_coastline_merged_result = merge_and_divided_by_threshold(mainland_coastline_dict, 100.0)
mainland_result = geopandas.GeoDataFrame.from_dict(mainland_coastline_merged_result, orient="index")
mainland_result["length"] = mainland_result.apply(lambda row: lonlat_length_in_km(row["geometry"]), axis=1)
print("==================================")
print("Merging and dividing coastline process completed")

merged_coastline = merged_coastline[merged_coastline.id != 9406157]
result = geopandas.GeoDataFrame(pandas.concat([merged_coastline,mainland_result], ignore_index=True))
result.to_file("data\\output\\coastline\\merged_and_divided_coastline.geojson", driver="GeoJSON")


import sys
from argparse import ArgumentParser

import geopandas
import shapely
from shapely import wkt
from src.enum.hofn_type import HofnType
from src.enum.mcc import National
import pandas

if __name__ == "__main__":

    def check_valid_round_precision(file):
        for i in range(5, 12):
            temp = file
            temp["POLYGON_STR"] = temp.apply(lambda row: shapely.wkt.loads(shapely.wkt.dumps(row["POLYGON_STR"], rounding_precision=i)), axis=1)
            if all(temp["POLYGON_STR"].is_valid):
                return i
            print(f"Round to {i} is invalid.")
        return 99

    parser = ArgumentParser()
    parser.add_argument("nation", type=str, help="Nation name.")
    parser.add_argument("--hofn_types", type=str, help="format: HofnType1 HofnType2 ...", nargs="+")
    parser.add_argument("--get_data", const=True, default=False, nargs="?")  # Set as a flag
    args = parser.parse_args()
    nation = args.nation
    hofn_types = args.hofn_types
    mcc = National[nation].get_mcc()

    files = {hofn_type: pandas.read_csv(f"data/output/{HofnType(hofn_type).name}/{nation}/{HofnType(hofn_type).name}.tsv", sep="\t") for hofn_type in hofn_types}
    for hofn_type, file in files.items():
        file["POLYGON_STR"] = file.apply(lambda row: wkt.loads(row["POLYGON_STR"]), axis=1)
        files[hofn_type] = geopandas.GeoDataFrame(file, geometry="POLYGON_STR")
    nt2_geo_polygon = pandas.DataFrame(columns=["POLYGON_ID", "POLYGON_NAME", "POLYGON_STR", "HOFN_TYPE", "ROAD_LEVEL"])
    # Validation:
    # 1. NO multi
    # 2. NO POLYGON_ID duplicate
    # 3. WKT coords round to 4 decimal place
    # 4. check POLYGON_STR is valid.
    for hofn_type, file in files.items():
        print(f"Current Hofn type: {hofn_type}")
        file["POLYGON_ID"] = file.apply(lambda row: f"{mcc}01{hofn_type}{row['POLYGON_ID']}", axis=1)  # Set polygon_id
        file.loc[file['POLYGON_NAME'].isnull(), "POLYGON_NAME"] = "UNKNOWN"
        # 1.
        multi_polygon_df = file[file.geometry.type == "MultiPolygon"]
        multi_linestring_df = file[file.geometry.type == "MultiLineString"]
        if multi_polygon_df.size or multi_linestring_df.size:
            print("===========================================================")
            print("multi polygon dataframe: ")
            print(multi_polygon_df, end="\n\n")
            print("===========================================================")
            print(multi_linestring_df, end="\n\n")
            print("Found multi-geometry showing above, please check")
            exit()
        print(f"{hofn_type} pass multi-type validation")
        # 2.
        if any(file["POLYGON_ID"].duplicated()):
            duplicate_in_student = file.duplicated(subset=['POLYGON_ID'])
            print("=========================================")
            print(file.loc[duplicate_in_student]["POLYGON_ID"], end="\n\n")
            print("Found duplicated polygon_id showing above, please check")
            exit()
        print(f"{hofn_type} pass unique id validation")
        # 3.
        # TODO: FUNCTION TO DYNAMIC
        # if hofn_type in ["01", "02", "05", "11"]:
        #     valid_decimal_place = check_valid_round_precision(file)
        #     file["POLYGON_STR"] = file.apply(lambda row: shapely.wkt.loads(shapely.wkt.dumps(row["POLYGON_STR"], rounding_precision=valid_decimal_place)), axis=1)
        #     print(f"{hofn_type} rounding precision is {valid_decimal_place}")
        # else:
        #     file["POLYGON_STR"] = file.apply(lambda row: shapely.wkt.loads(shapely.wkt.dumps(row["POLYGON_STR"], rounding_precision=5)), axis=1)
        #     print(f"{hofn_type} rounding precision is set to default {5}")
        # 4.
        if hofn_type in ["01", "05", "11"]:
            if not all(file["POLYGON_STR"].is_valid):
                invalid = file.is_valid
                print("=========================================")
                print(file.loc[~invalid][["POLYGON_ID", "POLYGON_STR"]], end="\n\n")
                print("Found invalid polygon showing above, please check")
                exit()
        elif hofn_type == "02":
            if not all(file["POLYGON_STR"].is_simple):
                invalid = file.is_valid
                print("=========================================")
                print(file.loc[~invalid][["POLYGON_ID", "POLYGON_STR"]], end="\n\n")
                print("Found invalid linestring showing above, please check")
                exit()
        print(f"{hofn_type} pass polygon validation")
        nt2_geo_polygon = pandas.concat([nt2_geo_polygon, file])
        nt2_geo_polygon.to_csv("data/output/NT2_GEO_POLYGON.tsv", sep="\t", index=False)
        print("Generate nt2 geo poloygon done.")

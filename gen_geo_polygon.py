from argparse import ArgumentParser
import geopandas
import shapely
from shapely import wkt
from src.enum import HofnType, National
import pandas

VERSION = 3
DEBUG_VERSION = 0


def get_geometry_rounding_limit(file: geopandas.GeoDataFrame):
    
    data = file.copy(deep=True)
    i = 5  # rounding precision must be larger than 5 for accuracy issue
    while i <= 7:
        data["geometry"] = data.geometry.apply(lambda geometry: wkt.loads(wkt.dumps(geometry, rounding_precision=5)))
        is_valid = all(data["geometry"].is_valid) if data["geometry"][0].geom_type == "Polygon" else all(data["geometry"].is_simple)
        if is_valid:
            return i
        else:
            return -1


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("mcc", type=str, help="mcc")
    parser.add_argument("hofn_types", type=str, help="format: 'HofnType1 HofnType2' ...")
    parser.add_argument("--get_data", const=True, default=False, nargs="?")  # Set as a flag
    args = parser.parse_args()
    mcc = args.mcc
    nation = National.get_country_by_mcc(mcc)
    hofn_types = args.hofn_types.split()

    files = {hofn_type: pandas.read_csv(f"data/output/{nation}/{HofnType(hofn_type).name}/{HofnType(hofn_type).name}.tsv", sep="\t") for hofn_type in hofn_types}
    for hofn_type, file in files.items():
        file["geometry"] = file.apply(lambda row: wkt.loads(row["geometry"]), axis=1)
        files[hofn_type] = geopandas.GeoDataFrame(file)
    nt2_geo_polygon = pandas.DataFrame(columns=["POLYGON_ID", "POLYGON_NAME", "POLYGON_STR", "HOFN_TYPE", "ROAD_LEVEL"])
    # Validation:
    for hofn_type, file in files.items():
        print(f"Current Hofn type: {hofn_type}")
        file["POLYGON_ID"] = file.apply(lambda row: f"{mcc}01{'0' + hofn_type if int(hofn_type) < 10 else hofn_type}{row['POLYGON_ID']}", axis=1)  # Set polygon_id
        file.loc[file['POLYGON_NAME'].isnull(), "POLYGON_NAME"] = "UNKNOWN"
        # 1. NO multi
        multi_polygon_df = file[file.geometry.apply(lambda x: x.type == "MultiPolygon")]
        multi_linestring_df = file[file.geometry.apply(lambda x: x.type == "MultiLineString")]
        if multi_polygon_df.size or multi_linestring_df.size:
            print("===========================================================")
            print("multi polygon dataframe: ")
            print(multi_polygon_df, end="\n\n")
            print("===========================================================")
            print(multi_linestring_df, end="\n\n")
            print("Found multi-geometry showing above, please check")
            exit()
        print(f"{hofn_type} pass multi-type validation")
        # 2.NO POLYGON_ID duplicate
        if any(file["POLYGON_ID"].duplicated()):
            duplicate_in_student = file.duplicated(subset=['POLYGON_ID'])
            print("=========================================")
            print(file.loc[duplicate_in_student]["POLYGON_ID"], end="\n\n")
            print("Found duplicated polygon_id showing above, please check")
            exit()
        print(f"{hofn_type} pass unique id validation")
        # 3. WKT coords round to 4 decimal place
        if hofn_type in ["1", "2", "5", "10", "11"]:
            precision_limit = get_geometry_rounding_limit(file)
            if precision_limit != -1:
                file["geometry"] = file.apply(lambda row: shapely.wkt.loads(shapely.wkt.dumps(row["geometry"], rounding_precision=precision_limit)), axis=1)
                print(f"{hofn_type} apply rounding precision {precision_limit}.")
            else:
                print(f"{hofn_type} no rounding, using original decimal points")
                pass
        else:
            file["geometry"] = file.apply(lambda row: shapely.wkt.loads(shapely.wkt.dumps(row["geometry"], rounding_precision=5)), axis=1)
            print(f"{hofn_type} rounding precision is set to default {5}")
        # 4. check POLYGON_STR is valid.
        if hofn_type in ["1", "5", "10", "11"]:
            if not all(file["geometry"].is_valid):
                invalid = file.is_valid
                print("=========================================")
                print(file.loc[~invalid][["POLYGON_ID", "geometry"]], end="\n\n")
                print("Found invalid polygon showing above, please check")
                exit()
        elif hofn_type == "2":
            if not all(file["geometry"].is_simple):
                invalid = file.is_simple
                print("=========================================")
                print(file.loc[~invalid][["POLYGON_ID", "geometry"]], end="\n\n")
                print("Found invalid linestring showing above, please check")
                exit()
        print(f"{hofn_type} pass polygon validation")
        file = file.rename(columns={"geometry": "POLYGON_STR"})
        nt2_geo_polygon = pandas.concat([nt2_geo_polygon, file])
    nt2_geo_polygon.to_csv(f"data/output/{nation}/NT2_GEO_POLYGON.csv", index=False)  # For debug purpose.
    nt2_geo_polygon.to_csv(f"data/output/{nation}/NT2_GEO_POLYGON.tsv", sep="\t", index=False)
    print("Generate nt2 geo poloygon done.")
    exit()

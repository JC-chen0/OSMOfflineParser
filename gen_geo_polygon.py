import sys
from argparse import ArgumentParser
from src.enum.hofn_type import HofnType
from src.enum.mcc import National
import pandas

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("nation", type=str, help="Nation name.")
    parser.add_argument("--hofn_types", type=str, help="format: HofnType1 HofnType2 ...", nargs="+")
    parser.add_argument("--get_data", const=True, default=False, nargs="?")  # Set as a flag
    args = parser.parse_args()
    nation = args.nation
    hofn_types = args.hofn_types
    mcc = National[nation].get_mcc()

    files = {hofn_type: pandas.read_csv(f"data/output/{HofnType(hofn_type).name}/{nation}/{HofnType(hofn_type).name}.tsv", sep="\t") for hofn_type in hofn_types}
    nt2_geo_polygon = pandas.DataFrame(columns=["POLYGON_ID", "POLYGON_NAME", "POLYGON_STR", "HOFN_TYPE", "ROAD_LEVEL"])
    for hofn_type, file in files.items():
        file["POLYGON_ID"] = file.apply(lambda row: f"{mcc}01{hofn_type}{row['POLYGON_ID']}", axis=1)
        nt2_geo_polygon = pandas.concat([nt2_geo_polygon, file])
    nt2_geo_polygon.to_csv("data/output/NT2_GEO_POLYGON.tsv", sep="\t", index=False)

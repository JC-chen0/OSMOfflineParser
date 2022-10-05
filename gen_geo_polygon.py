from argparse import ArgumentParser

import geopandas

from src.enum.hofntype import HofnType
from src.enum.mcc import National
import pandas

parser = ArgumentParser()
parser.add_argument("nation", type=str, help="Nation name.")
parser.add_argument("--hofn_types", type=str, help="format: HofnType1 HofnType2 ...", nargs="+")
args = parser.parse_args()
nation = args.nation
hofn_types = args.hofn_types
mcc = National[nation].get_mcc()
files = {hofn_type: pandas.read_csv(f"data/output/{HofnType(hofn_type).name}/{nation}/{HofnType(hofn_type).name}.tsv", sep="\t") for hofn_type in hofn_types}

for hofn_type, file in files.items():
    file.apply(lambda row: row["POLYGON_ID"] == f"{mcc}01{row['HOFN_TYPE']}{row['POLYGON_ID']}")

nt2_geo_polygon = geopandas.GeoDataFrame()
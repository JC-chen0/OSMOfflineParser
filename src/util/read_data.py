import geopandas


def read_file_and_rename_geometry(file_path: str):
    tmp = geopandas.read_file(file_path)
    tmp.rename_geometry("POLYGON_STR", inplace=True)
    return tmp


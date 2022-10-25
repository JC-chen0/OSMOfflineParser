from enum import Enum
import yaml

type_config = dict()
try:
    with open('config.yaml', 'r') as stream:
        type_config = yaml.safe_load(stream).get("HofnType")
except:
    pass


class HofnType(Enum):
    water = type_config.get("water")
    coastline = type_config.get("coastline")
    underground_mrt = type_config.get("underground_mrt")
    bridge = type_config.get("bridge")
    island = type_config.get("island")
    tunnel = type_config.get("tunnel")
    highway = type_config.get("highway")
    highway_in_desert = type_config.get("highway_in_desert")
    building = type_config.get("building")
    indoor_building_not_osm = type_config.get("indoor_building_not_osm")
    # ship_road = "10"
    ferry = type_config.get("ferry")
    village = type_config.get("village")
    forest = type_config.get("forest")
    field = type_config.get("field")
    railway = type_config.get("railway")
    funicular = type_config.get("funicular")

    def __init__(self, hofn_type):
        self.hofn_type = hofn_type

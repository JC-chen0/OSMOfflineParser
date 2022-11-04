from enum import Enum

import yaml

config = dict()
try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
        tag_config = config.get("tags")
        hofn_type_config = config.get("HofnType")
        national_config = config.get("national")
except:
    pass

class HofnType(Enum):
    water = hofn_type_config.get("water")
    coastline = hofn_type_config.get("coastline")
    underground_mrt = hofn_type_config.get("underground_mrt")
    bridge = hofn_type_config.get("bridge")
    island = hofn_type_config.get("island")
    tunnel = hofn_type_config.get("tunnel")
    highway = hofn_type_config.get("highway")
    highway_in_desert = hofn_type_config.get("highway_in_desert")
    building = hofn_type_config.get("building")
    indoor_building_not_osm = hofn_type_config.get("indoor_building_not_osm")
    # ship_road = "10"
    ferry = hofn_type_config.get("ferry")
    village = hofn_type_config.get("village")
    forest = hofn_type_config.get("forest")
    field = hofn_type_config.get("field")
    railway = hofn_type_config.get("railway")
    funicular = hofn_type_config.get("funicular")

    def __init__(self, hofn_type):
        self.hofn_type = hofn_type


class National(Enum):
    Taiwan = national_config.get("Taiwan").get("mcc"), national_config.get("Taiwan").get("relation")
    Singapore = national_config.get("Singapore").get("mcc"), national_config.get("Singapore").get("relation")
    Japan = national_config.get("Japan").get("mcc"), national_config.get("Japan").get("relation")
    UAE = national_config.get("UAE").get("mcc"), national_config.get("UAE").get("relation")
    Bosna = national_config.get("Bosna").get("mcc"), national_config.get("Bosna").get("relation")
    Mexico = national_config.get("Mexico").get("mcc"), national_config.get("Mexico").get("relation")
    Philippines = national_config.get("Philippines").get("mcc"), national_config.get("Philippines").get("relation")

    def __init__(self, mcc, relation):
        self.mcc = mcc
        self.relation = relation

    @classmethod
    def get_country_by_mcc(cls, mcc):
        for item in cls:
            if item.get_mcc() == mcc:
                return item.name

    def get_mcc(self):
        return self.mcc

    def get_relation_id(self):
        return self.relation


class Tag(Enum):
    water = tag_config.get("water")
    village = tag_config.get("village")
    coastline = tag_config.get("coastline")
    tunnel = tag_config.get("tunnel")
    # highway = {"highway": ["motorway", "trunk", "primary"]}  # highway control in get_data tags_config session
    highway = tag_config.get("highway")  # highway control in get_data tags_config session
    railway = tag_config.get("railway")
    ferry = tag_config.get("ferry")
    building = tag_config.get("building")

    def __init__(self, tags):
        self.tags = tags

    @staticmethod
    def get_levels(mode, level: dict):  # LEVEL: {"[LEVEL1_TAG]:1, [LEVEL2_TAG]:2 ... etc.}
        current_tags: list = Tag[mode].value.get(mode)
        return [level.get(i) for i in current_tags]

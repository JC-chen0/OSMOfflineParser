from enum import Enum

import yaml

tag_config = dict()
try:
    with open('config.yaml', 'r') as stream:
        tag_config = yaml.safe_load(stream).get("tags")
except:
    pass


class Tag(Enum):
    water = tag_config.get("water")
    village = tag_config.get("village")
    coastline = tag_config.get("coastline")
    tunnel = tag_config.get("tunnel")
    # highway = {"highway": ["motorway", "trunk", "primary"]}  # highway control in get_data config session
    highway = tag_config.get("highway")  # highway control in get_data config session
    railway = tag_config.get("railway")
    ferry = tag_config.get("ferry")
    building = tag_config.get("building")

    def __init__(self, tags):
        self.tags = tags

    @staticmethod
    def get_levels(mode, level: dict):  # LEVEL: {"[LEVEL1_TAG]:1, [LEVEL2_TAG]:2 ... etc.}
        current_tags: list = Tag[mode].value.get(mode)
        return [level.get(i) for i in current_tags]

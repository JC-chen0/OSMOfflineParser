from enum import Enum


class Tag(Enum):
    water = {"natural": "water", "landuse": "reservoir", "waterway": "riverbank"}
    village = {"landuse": "residential", "place": "village"}
    coastline = {"natural": "coastline"}
    tunnel = {"highway": "tunnel", "railway": "tunnel"}
    highway = {"highway": ["motorway", "trunk", "primary", "secondary", "tertiary"]}  # highway control in get_data config session
    # highway = {"highway": ["motorway", "trunk", "primary", "secondary", "tertiary"]}  # highway control in get_data config session
    railway = {"railway": "rail", "route": ["railway", "train"]}
    ferry = {"route": "ferry"}
    ship_road = {"route": "ferry"}
    building = {"building": "", "type": "building"}

    def __init__(self, tags):
        self.tags = tags

    @staticmethod
    def get_levels(mode, level: dict):  # LEVEL: {"[LEVEL1_TAG]:1, [LEVEL2_TAG]:2 ... etc.}
        current_tags: list = Tag[mode].value.get(mode)  # TODO: if LEVEL_DICT, the tags will be itself?
        return [level.get(i) for i in current_tags]

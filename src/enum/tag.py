from enum import Enum


class Tag(Enum):
    water = {"natural": "water", "landuse": "reservoir", "waterway": "riverbank"}
    village = {"landuse": "residential", "place": "village"}
    coastline = {"natural": "coastline"}
    tunnel = {"highway": "tunnel", "railway": "tunnel"}
    highway = {"highway": ["motorway", "trunk", "primary"]}  # highway control in get_data config session
    # highway = {"highway": ["motorway", "trunk", "primary", "secondary", "tertiary"]}  # highway control in get_data config session
    railway = {"railway": "rail", "route": ["railway", "train"]}

    def __init__(self, tags):
        self.tags = tags

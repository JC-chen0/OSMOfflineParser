from enum import Enum


class Tag(Enum):
    water = {}
    village = {}
    coastline = {}
    tunnel = {}
    highway = {}

    def __init__(self, tags):
        self.tags = tags

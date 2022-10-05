from enum import Enum


class HofnType(Enum):
    water = 1
    coastline = 2
    underground_mrt = 3
    bridge = 4
    island = 5
    tunnel = 6
    highway = 7
    highway_in_desert = 8
    indoor_building = 9
    indoor_building_not_osm = 9.1
    ship_road = 10
    village = 11
    forest = 13
    field = 13
    railway = 14
    cable_car = 15

    def __init__(self,hofn_type):
        self.hofn_type = hofn_type



from enum import Enum


class HofnType(Enum):
    water = "01"
    coastline = "02"
    underground_mrt = "03"
    bridge = "04"
    island = "05"
    tunnel = "06"
    highway = "07"
    highway_in_desert = "08"
    indoor_building = "09"
    indoor_building_not_osm = 9.1
    # ship_road = "10"
    ferry = "10"
    village = "11"
    forest = "13"
    field = "13"
    railway = "14"
    cable_car = "15"

    def __init__(self,hofn_type):
        self.hofn_type = hofn_type



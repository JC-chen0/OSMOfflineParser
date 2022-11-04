class HofnData:

    def __init__(self, POLYGON_ID, POLYGON_NAME, HOFN_TYPE, ROAD_LEVEL, geometry):
        self.POLYGON_ID = POLYGON_ID
        self.POLYGON_NAME = POLYGON_NAME
        self.HOFN_TYPE = HOFN_TYPE
        self.ROAD_LEVEL = ROAD_LEVEL
        self.geometry = geometry
    
    def __repr__(self):
        return str(self.POLYGON_ID)

class RelationMember:

    def __init__(self,id,type ,role):
        self.id = id
        self.type = type
        self.role = role 

    def __repr__(self):
        return str(self.id)

class Way:
    def __init__(self,id,name,geometry):
        self.id = id
        self.name = name
        self.geometry = geometry
    
    def __repr__(self):
        return str(f"{self.id}@{self.name}")
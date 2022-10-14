from enum import Enum


class National(Enum):
    Taiwan = "466", "449220"
    Singapore = "525", "536780"
    Saudi_Arabia = "420", "307584"
    Japan = "440", "382313"

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

from enum import Enum


class National(Enum):
    Taiwan = 466, "449220"
    Singapore = 525, "536780"
    Saudi_Arabia = 420, "307584"
    Japan = 440, "382313"

    def __init__(self, mcc, relation):
        self.mcc = mcc
        self.relation = relation

    def get_mcc(self):
        return self.mcc

    def get_relation_id(self):
        return self.relation

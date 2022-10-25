from enum import Enum

import yaml

national_config = dict()
try:
    with open('config.yaml', 'r') as stream:
        national_config = yaml.safe_load(stream).get("national")
except:
    pass


class National(Enum):
    Taiwan = national_config.get("Taiwan").get("mcc"), national_config.get("Taiwan").get("relation")
    Singapore = national_config.get("Singapore").get("mcc"), national_config.get("Singapore").get("relation")
    Japan = national_config.get("Japan").get("mcc"), national_config.get("Japan").get("relation")
    UAE = national_config.get("UAE").get("mcc"), national_config.get("UAE").get("relation")
    Bosna = national_config.get("Bosna").get("mcc"), national_config.get("Bosna").get("relation")
    Mexico = national_config.get("Mexico").get("mcc"), national_config.get("Mexico").get("relation")

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

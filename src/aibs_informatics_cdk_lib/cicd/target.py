from enum import Enum


class CDKStackTarget(str, Enum):
    PIPELINE = "pipeline"
    INFRA = "infra"

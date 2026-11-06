from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum


class IndexerType(StrEnum):
    DEFAULT = ""
    CALIBRATION = "Calibration"
    NORMALIZATION = "Normalization"
    REDUCTION = "Reduction"
    INSTRUMENT_PARAMETER = "InstrumentParameter"


def indexFilename(indexerType: IndexerType) -> str:
    return Config["indexer.filename"].format(indexerType=indexerType)

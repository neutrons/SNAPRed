from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.meta.Enum import StrEnum


class ReservedRunNumber(StrEnum):
    NATIVE: str = "000000"
    LITE: str = "000001"


class ReservedStateId(StrEnum):
    NATIVE: str = ObjectSHA(hex="0000000000000000").hex
    LITE: str = ObjectSHA(hex="0000000000000001").hex

    @classmethod
    def forRun(cls, runNumber):
        match runNumber:
            case ReservedRunNumber.NATIVE.value:
                return cls.NATIVE
            case ReservedRunNumber.LITE.value:
                return cls.LITE

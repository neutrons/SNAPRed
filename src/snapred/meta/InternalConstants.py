from typing import Tuple

from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.meta.Enum import StrEnum


class ReservedRunNumber(StrEnum):
    # The unmodified instruments: these have reserved states (see `ReservedStateId`).
    NATIVE: str = "000000"
    LITE: str = "000001"

    # Registered neutron data without a run number: for example, live data between runs, or before a run
    #   has been set up.  Its state is read from the data each time it is needed, and it is later than any
    #   real run, so that the latest calibration and normalization for its state apply.
    LIVE: str = "999999"

    @classmethod
    def unmodifiedInstrument(cls) -> Tuple[str, ...]:
        """The run numbers that stand for an unmodified instrument, with a reserved state."""
        return (cls.NATIVE.value, cls.LITE.value)


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

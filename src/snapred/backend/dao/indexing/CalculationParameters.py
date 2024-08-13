from datetime import datetime

from pydantic import field_validator
from snapred.backend.dao.indexing.Versioning import VersionedObject
from snapred.backend.dao.state.InstrumentState import InstrumentState

# NOTE: the request __init__ loads CalibrationExportRequest, which imports Calibration,
#       which imports this module, which causes a circular import situation.
#       In the future, need to remove the circular import so that full set of ingredients
#       can be preserved
# from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients


class CalculationParameters(VersionedObject, extra="allow"):
    """

    This is a base class to be used for the Calibration and Normalization objects,
    which are intended to record the state at the time of the calculation.

    This records the instrument state, the seed run used in the creation, the
    resolution (native/lite), the timestamp of the object's creation,
    a name for the calculation, and an associated version.

    """

    # inherits from VersionedObject:
    # - version: int

    instrumentState: InstrumentState
    seedRun: str
    useLiteMode: bool
    creationDate: datetime
    name: str

    @field_validator("seedRun", mode="before")
    @classmethod
    def validate_runNumber(cls, v):
        if isinstance(v, int):
            v = str(v)
        return v

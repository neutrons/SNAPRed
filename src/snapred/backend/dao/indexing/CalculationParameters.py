from datetime import datetime
import numpy
from pydantic import ConfigDict, field_validator

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
    useLiteMode: bool | numpy.bool_
    creationDate: datetime | str
    name: str

    @field_validator("seedRun", mode="before")
    @classmethod
    def validate_runNumber(cls, v):
        if isinstance(v, int):
            v = str(v)
        return v

    @field_validator("creationDate", mode="before")
    @classmethod
    def validate_creationDate(cls, v):
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        return v

    @field_validator("useLiteMode", mode="before")
    @classmethod
    def validate_useLiteMode(cls, v):
        # this allows the use of 'strict' mode
        if isinstance(v, numpy.bool_):
            v = bool(v)
        return v
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True, # numpy.bool_
        strict=True
    )

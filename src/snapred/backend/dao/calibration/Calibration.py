from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Calibration(BaseModel):
    """This is actually a group of parameters(mostly) used to peform a fitting.
    The contents of which are mostly static except for instrumentState.pixelgroupingparameters
    which should be moved off to a different object and persisted when the full calibration
    quality has been assessed and approved, along with the other derivative data.
    """

    instrumentState: InstrumentState
    seedRun: int
    creationDate: datetime
    name: str
    version: int = 0

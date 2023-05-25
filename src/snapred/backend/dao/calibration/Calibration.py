from datetime import datetime

from pydantic import BaseModel
from snapred.backend.dao.state.InstrumentState import InstrumentState


class Calibration(BaseModel):
    instrumentState: InstrumentState
    seedRun: int
    creationDate: datetime
    name: str

from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Calibration(BaseModel):
    """This is actually a group of parameters(mostly) used to peform a fitting.
    The contents of which should all be static
    """

    instrumentState: InstrumentState
    seedRun: int
    creationDate: datetime
    name: str
    version: int = 0

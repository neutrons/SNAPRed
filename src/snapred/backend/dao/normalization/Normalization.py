from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Normalization(BaseModel):
    instrumentState: InstrumentState
    seedRun: int
    creationDate: datetime
    name: str
    version: int = 0

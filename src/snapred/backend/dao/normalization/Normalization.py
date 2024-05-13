from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Normalization(BaseModel):
    """

    This class represents a normalization opject with essential attributes to track its origin,
    application, and metadata. It is designed to work within a system that requires understanding
    of the instrument state, facilitating data normalization processes in a structured and
    version-controlled manner.

    """

    instrumentState: InstrumentState
    seedRun: int
    useLiteMode: bool
    creationDate: datetime
    name: str
    version: int = 0

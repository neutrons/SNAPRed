from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator

from snapred.backend.dao.state import InstrumentState
from snapred.meta.Config import Config


class Normalization(BaseModel):
    """

    This class represents a normalization opject with essential attributes to track its origin,
    application, and metadata. It is designed to work within a system that requires understanding
    of the instrument state, facilitating data normalization processes in a structured and
    version-controlled manner.

    """

    instrumentState: InstrumentState

    # runNumber are `str` everywhere else, but `int` here?  I don't think so... :(
    seedRun: str

    useLiteMode: bool
    creationDate: datetime
    name: str
    version: int = Config["instrument.startingVersionNumber"]

    @field_validator("seedRun", mode="before")
    @classmethod
    def validate_runNumber(cls, v: Any) -> str:
        if not isinstance(v, str):
            v = str(v)
        return v

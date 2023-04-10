from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.StateConfig import StateConfig


class ReductionState(BaseModel):
    """Class to hold the instrument configuration."""

    instrumentConfig: InstrumentConfig
    stateConfig: StateConfig
    overrides: Optional[InstrumentConfig] = None

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v

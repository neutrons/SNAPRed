from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.StateConfig import StateConfig

from dataclasses import dataclass

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class ReductionState:
    """Class to hold the instrument configuration."""
    instrumentConfig: InstrumentConfig
    stateConfig: StateConfig
    overrides: InstrumentConfig

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v
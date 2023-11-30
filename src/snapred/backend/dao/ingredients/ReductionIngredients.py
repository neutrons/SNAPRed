from typing import List

from pydantic import BaseModel

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import PixelGroupingParameters


class ReductionIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    runConfig: RunConfig
    reductionState: ReductionState
    pixelGroupingParameters: List[PixelGroupingParameters]

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v

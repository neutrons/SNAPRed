from typing import List

from pydantic import BaseModel, Field

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup


class ReductionIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    # NOTE these depend on lite mode and focus group through PixelGroup
    # it is therefore necessary for the reduction service to have
    # access to the focus group to properly create these

    runConfig: RunConfig
    reductionState: ReductionState
    pixelGroup: PixelGroup

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v

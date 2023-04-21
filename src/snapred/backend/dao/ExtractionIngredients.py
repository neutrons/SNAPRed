from pydantic import BaseModel
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig


class ExtractionIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    runConfig: RunConfig
    extractionState: ReductionState

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v

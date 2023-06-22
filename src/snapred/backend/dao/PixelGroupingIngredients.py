from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    calibrationState: Calibration
    instrumentDefinitionFile: str
    groupingFile: str

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v

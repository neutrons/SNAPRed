from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    calibrationState: Calibration
    instrumentDefinitionFile: str
    groupingFile: str

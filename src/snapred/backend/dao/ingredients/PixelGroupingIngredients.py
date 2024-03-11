from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.meta.Config import Config


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    instrumentState: InstrumentState

    groupingScheme: Optional[str]

    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]

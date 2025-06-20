from pydantic import BaseModel, Field

from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.meta.Config import Config


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    instrumentState: InstrumentState

    groupingScheme: str | None = None

    nBinsAcrossPeakWidth: int = Field(default_factory=lambda: Config["calibration.diffraction.nBinsAcrossPeakWidth"])

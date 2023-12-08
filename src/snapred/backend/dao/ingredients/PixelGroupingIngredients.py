from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    instrumentState: InstrumentState

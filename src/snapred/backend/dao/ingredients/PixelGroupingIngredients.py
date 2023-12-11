from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState

# TODO does this class need to exist?


class PixelGroupingIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    instrumentState: InstrumentState
    groupingScheme: Optional[str]

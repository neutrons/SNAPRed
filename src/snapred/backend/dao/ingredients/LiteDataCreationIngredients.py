from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState


class LiteDataCreationIngredients(BaseModel):
    """Class to hold the ingredients necessary for lite-data conversion."""

    instrumentState: InstrumentState | None = None

    toleranceOverride: float | None = None

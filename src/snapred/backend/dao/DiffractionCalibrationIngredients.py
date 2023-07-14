from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    runConfig: RunConfig
    instrumentState: InstrumentState
    focusGroup: FocusGroup

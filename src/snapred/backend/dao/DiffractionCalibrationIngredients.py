from pydantic import BaseModel

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    dBin: float
    runConfig: RunConfig
    extractionState: ReductionState
    instrumentState: InstrumentState
    focusGroup: FocusGroup

    # rawDataPath
    # calibrantCIF
    # stateFilename
    # groupingFile
    #

from pydantic import BaseModel

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig


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

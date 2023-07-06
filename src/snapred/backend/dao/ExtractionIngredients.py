from pydantic import BaseModel

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig


class ExtractionIngredients(BaseModel):
    """Class to hold the instrument configuration."""

    runConfig: RunConfig
    extractionState: ReductionState

    # rawDataPath
    # calibrantCIF
    # stateFilename
    # groupingFile
    # 
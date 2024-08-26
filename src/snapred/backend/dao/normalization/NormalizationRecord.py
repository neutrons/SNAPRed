from typing import Any, List

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.Limit import Limit
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NormalizationRecord(BaseModel):
    """

    This class is crucial for tracking the specifics of each normalization step, facilitating
    reproducibility and data management within scientific workflows. It serves as a comprehensive
    record of the parameters and context of normalization operations, contributing to the integrity
    and utility of the resulting data.

    """

    runNumber: str
    useLiteMode: bool
    backgroundRunNumber: str
    smoothingParameter: float
    peakIntensityThreshold: float
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    calibration: Calibration
    workspaceNames: List[WorkspaceName] = []
    version: int = Config["instrument.startingVersionNumber"]
    crystalDBounds: Limit[float]
    normalizationCalibrantSamplePath: str

    @field_validator("runNumber", "backgroundRunNumber", mode="before")
    @classmethod
    def validate_runNumber(cls, v: Any) -> Any:
        if isinstance(v, int):
            v = str(v)
        return v

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

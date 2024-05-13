from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration


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
    calibration: Calibration
    workspaceNames: List[str] = []
    version: Optional[int]
    dMin: float

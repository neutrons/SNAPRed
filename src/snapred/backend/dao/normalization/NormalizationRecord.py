from typing import List

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.Record import Record


class NormalizationRecord(Record):
    """

    This class is crucial for tracking the specifics of each normalization step, facilitating
    reproducibility and data management within scientific workflows. It serves as a comprehensive
    record of the parameters and context of normalization operations, contributing to the integrity
    and utility of the resulting data.

    """

    # inhereited from Record
    runNumber: str
    useLiteMode: bool
    version: int

    # specific to normalization records
    backgroundRunNumber: str
    smoothingParameter: float
    peakIntensityThreshold: float
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    calibration: Calibration
    workspaceNames: List[str] = []

    dMin: float

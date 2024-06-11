from typing import List

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.indexing.Record import Record
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NormalizationRecord(Record):
    """

    This class is crucial for tracking the specifics of each normalization step, facilitating
    reproducibility and data management within scientific workflows. It serves as a comprehensive
    record of the parameters and context of normalization operations, contributing to the integrity
    and utility of the resulting data.

    """

    # inherits from Record
    # - runNumber
    # - useLiteMode
    # - version

    # specific to normalization records
    backgroundRunNumber: str
    smoothingParameter: float
    peakIntensityThreshold: float
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    calibration: Calibration
    workspaceNames: List[WorkspaceName] = []

    dMin: float

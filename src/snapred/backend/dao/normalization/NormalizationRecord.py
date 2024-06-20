from typing import Any, List

from pydantic import field_validator

from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, Version
from snapred.backend.dao.normalization.Normalization import Normalization
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
    # override this to point at the correct daughter class
    calculationParameters: Normalization

    # specific to normalization records
    backgroundRunNumber: str
    smoothingParameter: float
    peakIntensityThreshold: float
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    workspaceNames: List[WorkspaceName] = []
    calibrationVersionUsed: Version = VERSION_DEFAULT

    dMin: float

    # must also parse integers as background run numbers
    @field_validator("backgroundRunNumber", mode="before")
    @classmethod
    def validate_backgroundRunNumber(cls, v: Any) -> Any:
        return cls.validate_runNumber(v)

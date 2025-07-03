from typing import List

from pydantic import BaseModel, Field

from snapred.backend.dao.indexing.Versioning import VERSION_START, Version
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NormalizationAssessmentResponse(BaseModel, arbitrary_types_allowed=True):
    calculationParameters: Normalization

    # specific to normalization records
    backgroundRunNumber: str
    smoothingParameter: float
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    workspaceNames: List[WorkspaceName] = []
    calibrationVersionUsed: Version = Field(default_factory=VERSION_START)
    crystalDBounds: Limit[float]
    normalizationCalibrantSamplePath: str

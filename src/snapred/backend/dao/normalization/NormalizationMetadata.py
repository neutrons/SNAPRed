from typing import Any, List

from pydantic import BaseModel, field_serializer

from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VersionedObject
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NormalizationMetadata(BaseModel, extra="ignore"):
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
    # detectorPeaks: List[DetectorPeak] # TODO: need to save this for reference during reduction
    workspaceNames: List[WorkspaceName] = []
    calibrationVersionUsed: int = VERSION_DEFAULT
    crystalDBounds: Limit[float]
    normalizationCalibrantSamplePath: str

    @field_serializer("calibrationVersionUsed", when_used="json")
    def write_user_defaults(self, value: Any):  # noqa ARG002
        return VersionedObject.writeVersion(self.calibrationVersionUsed)

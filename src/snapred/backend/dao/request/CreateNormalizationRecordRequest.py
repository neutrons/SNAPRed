from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CreateNormalizationRecordRequest(BaseModel, extra="forbid"):
    """
    The needed data to create a NormalizationRecord.
    """

    runNumber: str
    useLiteMode: bool
    version: Optional[int] = None
    calculationParameters: Normalization
    backgroundRunNumber: str
    smoothingParameter: float
    peakIntensityThreshold: float
    workspaceNames: List[WorkspaceName] = []
    calibrationVersionUsed: Optional[int] = VERSION_DEFAULT
    crystalDBounds: Limit[float]

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

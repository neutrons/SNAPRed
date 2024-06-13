from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionIngredients(BaseModel):
    """Data class to hold the ingredients for each subrecipe of reduction and itself"""

    maskList: List[WorkspaceName]
    pixelGroups: List[PixelGroup]
    detectorPeaksMany: List[List[GroupPeakList]]

    # these should come from calibration / normalization records
    smoothingParameter: float
    calibrantSamplePath: str
    peakIntensityThreshold: float

    # These are just to interface with child recipes easier
    pixelGroup: Optional[PixelGroup] = None
    detectorPeaks: Optional[List[GroupPeakList]] = None

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

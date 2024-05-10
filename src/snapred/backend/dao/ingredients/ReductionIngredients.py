from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.state.PixelGroup import PixelGroup


class ReductionIngredients(BaseModel):
    """These are reduction ingredients.  Use them wisely."""

    maskList: List[str]
    pixelGroups: List[PixelGroup]
    detectorPeaksMany: List[List[GroupPeakList]]
    smoothingParameter: float

    # These are just to interface with child recipes easier
    pixelGroup: Optional[PixelGroup] = None
    detectorPeaks: Optional[List[GroupPeakList]] = None
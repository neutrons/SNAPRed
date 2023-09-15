from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.DetectorPeak import DetectorPeak


class GroupPeakList(BaseModel):
    """Class to hold diffraction peak positions of one group"""

    peaks: List[DetectorPeak]
    groupID: int
    maxfwhm: Optional[float]  # this is in d-spacing, not bins, != 5

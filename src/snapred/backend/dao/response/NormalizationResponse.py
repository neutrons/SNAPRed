from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList


class NormalizationResponse(BaseModel):
    """

    This class serves as a structured representation of the outcomes from a vanadium-based
    normalization procedure, encapsulating the various states of vanadium data (corrected,
    focused, and smoothed) alongside detected peaks across detectors. It provides a comprehensive
    overview of the results, facilitating further analysis or reporting within scientific
    workflows. The detailed encapsulation of each state of vanadium data and the collected peak
    lists make this class an invaluable asset for post-normalization process evaluation and
    decision-making.

    """

    correctedVanadium: str
    focusedVanadium: str
    smoothedVanadium: str
    detectorPeaks: List[GroupPeakList]

    # This is returned on first go of the normalization process
    # Follow up recalc operations reuse this response object
    # but do not need to return calibrationRunNumber
    calibrationRunNumber: Optional[str] = None

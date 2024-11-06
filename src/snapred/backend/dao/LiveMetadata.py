import numpy as np
from typing import ClassVar
from datetime import datetime
from pydantic import BaseModel

from snapred.backend.dao.state import DetectorState


class LiveMetadata(BaseModel):
    """Metadata about any in-progress data acquisition."""
    
    # Implementation notes:
    #
    #   * If there's no run currently active, the `runNumber` will be `INACTIVE_RUN`.
    #
    #   * Metadata may change at any time.  For example, the run may terminate and become inactive.
    #     For this reason, in most cases this DAO should never be cached.
    #
    
    INACTIVE_RUN: ClassVar[int] = 0
    
    runNumber: str
    
    startTime: datetime
    endTime: datetime
    
    detectorState: DetectorState
 
    protonCharge: float
 
    def hasActiveRun(self):
        return int(runNumber) != INACTIVE_RUN

    def beamState(self):
        return self.protonCharge > 0.0

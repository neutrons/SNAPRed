from datetime import datetime
from typing import ClassVar

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

    ## Special Live-listener <start time> strings in ISO8601 format:
    #  Return data from _now_ (<epoch zero>):
    FROM_NOW_ISO8601: ClassVar[str] = "1990-01-01T00:00:00"

    #  Return data from start of run (<epoch +1 second>):
    FROM_START_ISO8601: ClassVar[str] = "1990-01-01T00:00:01"

    INACTIVE_RUN: ClassVar[int] = 0

    runNumber: str

    startTime: datetime
    endTime: datetime

    detectorState: DetectorState | None

    protonCharge: float

    def hasActiveRun(self):
        return int(self.runNumber) != LiveMetadata.INACTIVE_RUN

    def beamState(self):
        return self.protonCharge > 0.0

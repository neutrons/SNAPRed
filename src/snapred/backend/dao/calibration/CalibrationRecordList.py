from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord


class CalibrationRecordList(BaseModel):
    """A list of calibration records"""

    records: List[CalibrationRecord]

from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo


class DiffractionCalibrant(BaseModel):
    runNumber: str
    filename: str
    diffCalPath: str
    name: Optional[str]
    latticeParameters: Optional[str]  # though it is a csv string of floats
    reference: Optional[str]
    crystallographicInfo: CrystallographicInfo

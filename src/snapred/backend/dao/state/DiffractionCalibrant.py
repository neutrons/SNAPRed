from typing import Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo


class DiffractionCalibrant(BaseModel):
    runNumber: str
    filename: str
    diffCalPath: str
    name: Optional[str] = None
    latticeParameters: Optional[str] = None  # though it is a csv string of floats
    reference: Optional[str] = None
    crystallographicInfo: Optional[CrystallographicInfo] = None
    fSquaredThreshold: Optional[float] = None

    model_config = ConfigDict(strict=True)

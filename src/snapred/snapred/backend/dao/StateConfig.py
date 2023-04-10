from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant


class StateConfig(BaseModel):
    diffractionCalibrant: DiffractionCalibrant
    emptyInstrumentRunNumber: int
    normalizationCalibrant: NormalizationCalibrant
    geometryCalibrationFileName: Optional[str]
    calibrationAuthor: str
    calibrationDate: str  # Should this be of type datetime?
    focusGroups: List[FocusGroup]
    isLiteMode: bool
    rawVanadiumCorrectionFileName: str
    calibrationMaskFileName: Optional[str]
    stateId: str
    tofBin: float
    tofMax: float
    tofMin: float
    version: str
    wallclockTof: float
    temporalProximity: Optional[
        int
    ]  # diffrence between current run number and calibration run number, - == before, + == after

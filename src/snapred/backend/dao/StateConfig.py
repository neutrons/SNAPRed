from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup

from typing import List
from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class StateConfig:
   diffractionCalibrant: DiffractionCalibrant
   emptyInstrumentRunNumber: int
   normalizationCalibrant: NormalizationCalibrant
   geometryCalibrationFileName: str
   calibrationAuthor: str 
   calibrationDate: str  # Should this be of type datetime?
   focusGroups: List[FocusGroup]
   isLiteMode: bool
   rawVanadiumCorrectionFileName: str
   calibrationMaskFileName: str
   stateId: str
   tofBin: float
   tofMax: float
   tofMin: float
   version: str
   wallclockTof: float
   temporalProximity: int # diffrence between current run number and calibration run number, - == before, + == after
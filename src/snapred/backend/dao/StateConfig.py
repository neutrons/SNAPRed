from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant


class StateConfig(BaseModel):
    calibration: Calibration
    diffractionCalibrant: Optional[DiffractionCalibrant]
    normalizationCalibrant: Optional[NormalizationCalibrant]
    focusGroups: List[FocusGroup]  # PixelGroupingParameters
    stateId: str  # generated.
    tofBin: float  # instrConfig.delTOverT / instrConfig.NBins
    tofMax: float  # ParticleBound
    tofMin: float  # ParticleBound

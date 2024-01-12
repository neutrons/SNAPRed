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
    focusGroups: List[FocusGroup]  # from the group map
    stateId: str  # generated.

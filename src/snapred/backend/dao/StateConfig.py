from typing import Any, List, Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant


class StateConfig(BaseModel):
    calibration: Calibration
    diffractionCalibrant: Optional[DiffractionCalibrant]
    normalizationCalibrant: Optional[NormalizationCalibrant]
    rawVanadiumCorrectionFileName: str  # Needs to be removed when Normalization changes go in
    vanadiumFilePath: str  # Needs to be removed when Normalization changes go in
    focusGroups: List[FocusGroup]  # PixelGroupingParameters
    stateId: ObjectSHA  # generated.

    @validator("stateId", pre=True, allow_reuse=True)
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v)
        return v

# TODO this can probably be relaced in the code with FarmFreshIngredients
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.Limit import Pair
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum


class DiffractionCalibrationRequest(BaseModel, extra="forbid"):
    """

    The DiffractionCalibrationRequest class is designed to kick-start the calibration process
    for a specific run by comparing it against known crystallographic data from a cif file.
    It includes the runNumber, calibrantSamplePath, and the focusGroup involved, alongside
    settings like useLiteMode for simplified processing and a series of calibration parameters
    such as crystalDMin, crystalDMax, peakFunction, and thresholds for convergence and peak
    intensity. These parameters are pre-configured with default values from the system's
    configuration, ensuring a consistent and precise approach to diffraction calibration.

    """

    runNumber: str
    calibrantSamplePath: str
    focusGroup: FocusGroup
    useLiteMode: bool
    crystalDMin: float = Config["constants.CrystallographicInfo.crystalDMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.crystalDMax"]
    peakFunction: SymmetricPeakEnum = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]
    convergenceThreshold: float = Config["calibration.diffraction.convergenceThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    maximumOffset: float = Config["calibration.diffraction.maximumOffset"]
    fwhmMultipliers: Pair[float] = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
    maxChiSq: float = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    @field_validator("fwhmMultipliers", mode="before")
    @classmethod
    def validate_fwhmMultipliers(cls, v: Any) -> Pair[float]:
        if isinstance(v, dict):
            v = Pair[float](**v)
        if not isinstance(v, Pair[float]):
            # Coerce Generic[T]-derived type
            v = Pair[float](**v.dict())
        return v

    model_config = ConfigDict(extra="forbid")

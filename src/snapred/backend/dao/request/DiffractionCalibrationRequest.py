# TODO this can probably be relaced in the code with FarmFreshIngredients

from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum


class DiffractionCalibrationRequest(BaseModel):
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
    crystalDMin: float = Config["constants.CrystallographicInfo.dMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.dMax"]
    peakFunction: SymmetricPeakEnum = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]
    convergenceThreshold: float = Config["calibration.diffraction.convergenceThreshold"]
    peakIntensityThreshold: float = Config["calibration.diffraction.peakIntensityThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    maximumOffset: float = Config["calibration.diffraction.maximumOffset"]
    fwhmMultiplierLimit: Limit[float] = Limit(
        minimum=Config["calibration.parameters.default.FWHMMultiplier"][0],
        maximum=Config["calibration.parameters.default.FWHMMultiplier"][1],
    )

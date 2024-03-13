from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class NormalizationCalibrationRequest(BaseModel):
    """

    This class encapsulates all the necessary parameters to request a normalization process,
    including default values from application.yml for ease of use and consistency across runs. It
    is designed to provide a comprehensive and customizable framework for requesting normalization
    calibrations, facilitating precise and tailored calibration processes.

    """

    runNumber: str
    backgroundRunNumber: str
    useLiteMode: bool = True  # TODO: Ensure this is activated in the view and workflow context.
    focusGroup: FocusGroup
    calibrantSamplePath: str
    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    crystalDMin: float = Config["constants.CrystallographicInfo.dMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.dMax"]
    peakIntensityThreshold: float = Config["constants.PeakIntensityFractionThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class SmoothDataExcludingPeaksRequest(BaseModel):
    """

    This class encapsulates all necessary parameters for a request to smooth data while excluding
    peaks, including default values from configuration for ease of use and consistency across runs.
    It offers a comprehensive framework for specifying the details of a data smoothing operation,
    ensuring that significant data features are preserved while reducing noise.

    """

    runNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup
    calibrantSamplePath: str
    inputWorkspace: str
    outputWorkspace: str
    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    crystalDMin: float = Config["constants.CrystallographicInfo.crystalDMin"]
    crystalDMax: float = Config["constants.CrystallographicInfo.crystalDMax"]

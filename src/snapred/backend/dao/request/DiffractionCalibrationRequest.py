from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class DiffractionCalibrationRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    runNumber: str
    calibrantSamplePath: str
    focusGroup: FocusGroup
    useLiteMode: bool
    convergenceThreshold: Optional[float] = Config["calibration.diffraction.convergenceThreshold"]
    peakIntensityThreshold: Optional[float] = Config["calibration.diffraction.peakIntensityThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    maximumOffset: float = Config["calibration.diffraction.maximumOffset"]

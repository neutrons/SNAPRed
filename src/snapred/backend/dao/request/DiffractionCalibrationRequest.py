from typing import Literal, Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.PeakFunctionEnum import PeakFunctionEnum


class DiffractionCalibrationRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    runNumber: str
    calibrantSamplePath: str
    focusGroup: FocusGroup
    useLiteMode: bool
    peakFunction: PeakFunctionEnum = Config["calibration.diffraction.peakFunction"]
    convergenceThreshold: Optional[float] = Config["calibration.diffraction.convergenceThreshold"]
    peakIntensityThreshold: Optional[float] = Config["calibration.diffraction.peakIntensityThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    maximumOffset: float = Config["calibration.diffraction.maximumOffset"]

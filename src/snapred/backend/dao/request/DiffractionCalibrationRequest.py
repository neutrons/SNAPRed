from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config


class DiffractionCalibrationRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    runNumber: str
    cifPath: str
    focusGroupPath: str
    useLiteMode: bool
    groupingScheme: Optional[str]  # TODO
    convergenceThreshold: Optional[float] = Config["calibration.diffraction.convergenceThreshold"]
    peakIntensityThreshold: Optional[float] = Config["calibration.diffraction.peakIntensityThreshold"]
    nBinsAcrossPeakWidth: Optional[int] = Config["calibration.diffraction.nBinsAcrossPeakWidth"]

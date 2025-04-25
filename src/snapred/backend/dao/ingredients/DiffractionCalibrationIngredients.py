from typing import List

from pydantic import BaseModel, Field

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum


class DiffractionCalibrationIngredients(BaseModel, extra="forbid"):
    """

    The DiffractionCalibrationIngredients class encapsulates all the necessary components for
    conducting diffraction calibration. It contains a runConfig for the calibration run settings,
    a pixelGroup specifying the group of pixels under consideration, and a list of groupedPeakLists
    detailing the peaks identified in each group. Additionally, it defines a convergenceThreshold
    for calibration accuracy, a peakFunction selected based on system configuration for modeling
    the peaks, and a maxOffset limit for calibration adjustments.

    """

    runConfig: RunConfig
    pixelGroup: PixelGroup
    groupedPeakLists: List[GroupPeakList]
    convergenceThreshold: float = Field(default_factory = lambda: float(Config["calibration.diffraction.convergenceThreshold"]))
    peakFunction: SymmetricPeakEnum = Field(default_factory = lambda: SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]])
    maxOffset: float = Field(default_factory = lambda: Config["calibration.diffraction.maximumOffset"])
    maxChiSq: float = Field(default_factory = lambda: Config["constants.GroupDiffractionCalibration.MaxChiSq"])
    removeBackground: bool = False

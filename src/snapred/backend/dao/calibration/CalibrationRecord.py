from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class CalibrationRecord(BaseModel):
    """This records the inputs used to generate a given calibration version"""

    runNumber: str
    crystalInfo: CrystallographicInfo
    calibrationFittingIngredients: Calibration
    pixelGroups: Optional[List[PixelGroup]] # TODO: really shouldnt be optional, will be when sns data fixed
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaceNames: List[WorkspaceName]
    version: Optional[int]

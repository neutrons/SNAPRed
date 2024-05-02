from typing import Dict, List, Optional

from pydantic import BaseModel

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationRecord(BaseModel):
    """

    The CalibrationRecord class, serves as a comprehensive log of the inputs and parameters employed
    to produce a specific version of a calibration. It systematically records the runNumber, detailed
    crystalInfo from CrystallographicInfo, and the calibration parameters from Calibration. Additionally,
    it may include pixelGroups, a list of PixelGroup objects (intended to be mandatory in future updates),
    and focusGroupCalibrationMetrics derived from FocusGroupMetric to evaluate calibration quality. The
    workspaces dictionary maps WorkspaceType to lists of WorkspaceName, organizing the associated Mantid
    workspace names by type. An optional version number allows for tracking the calibration evolution over
    time.

    """

    runNumber: str
    crystalInfo: CrystallographicInfo
    isLite: bool
    calibrationFittingIngredients: Calibration
    pixelGroups: Optional[List[PixelGroup]]  # TODO: really shouldn't be optional, will be when sns data fixed
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaces: Dict[WorkspaceType, List[WorkspaceName]]
    version: Optional[int]

from typing import Annotated, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationAssessmentResponse(BaseModel):
    """

    The CalibrationAssessmentResponse class serves as a response model specifically designed
    for summarizing the outcomes of calibration assessments. It incorporates a CalibrationRecord
    to detail the calibration performed and includes a list of metricWorkspaces, which are strings
    identifying the workspaces where the calibration metrics are stored.

    """

    version: Version = VersionState.NEXT
    calculationParameters: Calibration
    crystalInfo: CrystallographicInfo
    pixelGroups: Optional[List[PixelGroup]] = None
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaces: Dict[Annotated[WorkspaceType, Field(use_enum_values=True)], List[WorkspaceName]]
    metricWorkspaces: List[str]

    # To allow WorkspaceName to be used as a type
    model_config = ConfigDict(arbitrary_types_allowed=True)

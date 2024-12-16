from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CreateCalibrationRecordRequest(BaseModel, extra="forbid"):
    """

    The information needed to create a calibration record.

    """

    runNumber: str
    useLiteMode: bool
    version: Version = VersionState.NEXT
    calculationParameters: Calibration
    crystalInfo: CrystallographicInfo
    pixelGroups: Optional[List[PixelGroup]] = None
    focusGroupCalibrationMetrics: FocusGroupMetric
    workspaces: Dict[WorkspaceType, List[WorkspaceName]]

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

from typing import Annotated, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.Hook import Hook
from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CreateCalibrationRecordRequest(BaseModel, extra="forbid", strict=False):
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
    workspaces: Dict[Annotated[WorkspaceType, Field(use_enum_values=True)], List[WorkspaceName]]

    hooks: Dict[str, List[Hook]] | None = None

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True
    )

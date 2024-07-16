from typing import List

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class FitMultiplePeaksRequest(BaseModel):
    inputWorkspace: WorkspaceName
    outputWorkspaceGroup: WorkspaceName
    detectorPeaks: List[GroupPeakList]
    peakFunction: SymmetricPeakEnum = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

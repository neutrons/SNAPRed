from typing import Optional

from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class FocusSpectraRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup
    preserveEvents: bool

    inputWorkspace: str
    groupingWorkspace: str
    maskWorkspace: WorkspaceName | None = None
    outputWorkspace: WorkspaceName | None = None
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
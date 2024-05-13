from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class FocusSpectraRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup

    inputWorkspace: str
    groupingWorkspace: str
    outputWorkspace: Optional[str]

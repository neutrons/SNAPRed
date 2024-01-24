from matplotlib import use
from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class FocusSpectraRequest(BaseModel):
    inputWorkspace: str
    groupingWorkspace: str
    runNumber: str
    focusGroup: FocusGroup
    useLiteMode: bool = True
    outputWorkspace: str

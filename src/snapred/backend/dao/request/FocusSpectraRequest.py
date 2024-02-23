from matplotlib import use
from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class FocusSpectraRequest(BaseModel):
    runNumber: str
    useLiteMode: bool = True
    focusGroup: FocusGroup

    inputWorkspace: str
    groupingWorkspace: str
    outputWorkspace: str

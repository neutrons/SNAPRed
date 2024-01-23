from typing import Optional

from matplotlib import use
from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup


class FocusSpectraRequest(BaseModel):
    inputWorkspace: str
    groupingWorkspace: str
    runNumber: str
    focusGroup: FocusGroup
    useLiteMode: bool = True
    outputWorkspace: str
    groupingWorkspace: str
    pixelGroup: Optional[PixelGroup]

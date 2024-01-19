from typing import Optional

from matplotlib import use
from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroup import PixelGroup


class FocusSpectraRequest(BaseModel):
    inputWorkspace: str
    outputWorkspace: str
    groupingWorkspace: str
    pixelGroup: Optional[PixelGroup]

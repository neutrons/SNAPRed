from typing import List

from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class FocusGroupParameters(BaseModel):
    focusGroupName: str
    pixelGroupingParameters: List[PixelGroupingParameters]

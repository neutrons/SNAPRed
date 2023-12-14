# TODO delete

from typing import List

from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class FocusGroupParameters(BaseModel):
    """
    A join object that maps a list of Pixel Grouping Parameters to a focus group.
    """

    focusGroupName: str
    pixelGroupingParameters: List[PixelGroupingParameters]

from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroup import PixelGroup


class RebinFocussedGroupDataIngredients(BaseModel):
    pixelGroup: PixelGroup
    preserveEvents: bool = False

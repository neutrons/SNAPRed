from pydantic import BaseModel

from snapred.backend.dao.state.PixelGroup import PixelGroup


class ApplyNormalizationIngredients(BaseModel):
    pixelGroup: PixelGroup

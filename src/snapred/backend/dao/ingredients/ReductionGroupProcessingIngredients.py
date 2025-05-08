from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.state.PixelGroup import PixelGroup


class ReductionGroupProcessingIngredients(BaseModel):
    pixelGroup: PixelGroup
    preserveEvents: bool

    model_config = ConfigDict(
        extra="forbid",
    )

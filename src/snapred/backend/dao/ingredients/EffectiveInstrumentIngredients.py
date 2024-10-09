from pydantic import BaseModel, ConfigDict

from snapred.backend.dao.state.PixelGroup import PixelGroup


class EffectiveInstrumentIngredients(BaseModel):
    unmaskedPixelGroup: PixelGroup

    model_config = ConfigDict(
        extra="forbid",
    )

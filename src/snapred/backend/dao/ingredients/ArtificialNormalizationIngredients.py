from pydantic import BaseModel, Field

from snapred.meta.Config import Config


class ArtificialNormalizationIngredients(BaseModel):
    """
    Class to hold ingredients for the creation of artificial normalization data.
    """

    peakWindowClippingSize: int = Field(
        default_factory=lambda: Config["constants.ArtificialNormalization.peakWindowClippingSize"]
    )
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True

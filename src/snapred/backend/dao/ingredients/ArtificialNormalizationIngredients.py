from pydantic import BaseModel

from snapred.meta.Config import Config


class ArtificialNormalizationIngredients(BaseModel):
    """
    Class to hold ingredients for the creation of artificial normalization data.
    """

    peakWindowClippingSize: int = Config["constants.ArtiificialNormalization.peakWindowClippingSize"]
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True

from pydantic import BaseModel


class ArtificialNormalizationIngredients(BaseModel):
    """
    Class to hold ingredients for the creationon of artificial normalization data.
    """

    peakWindowClippingSize: int = 10
    smoothingParameter: float
    decreaseParameter: bool = True
    lss: bool = True

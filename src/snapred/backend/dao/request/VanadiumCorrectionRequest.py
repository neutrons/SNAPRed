from pydantic import BaseModel

from snapred.backend.dao.ingredients import NormalizationIngredients


class VanadiumCorrectionRequest(BaseModel):
    inputWorkspace: str
    backgroundWorkspace: str
    outputWorkspace: str
    ingredients: NormalizationIngredients

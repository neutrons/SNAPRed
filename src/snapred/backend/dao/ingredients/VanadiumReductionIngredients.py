from pydantic import BaseModel

from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.RunConfig import RunConfig


class VanadiumReductionIngredients(BaseModel):
    run: RunConfig
    smoothIngredients: SmoothDataExcludingPeaksIngredients

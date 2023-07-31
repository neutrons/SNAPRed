from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients


class VanadiumReductionIngredients(BaseModel):
    run: RunConfig
    smoothIngredients: SmoothDataExcludingPeaksIngredients

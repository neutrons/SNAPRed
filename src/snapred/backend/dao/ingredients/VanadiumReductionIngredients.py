from pydantic import BaseModel

from snapred.backend.dao.ingredients.PeakIngredients import PeakIngredients
from snapred.backend.dao.RunConfig import RunConfig


class VanadiumReductionIngredients(BaseModel):
    run: RunConfig
    smoothIngredients: PeakIngredients

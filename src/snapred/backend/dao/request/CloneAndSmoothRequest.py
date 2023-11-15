from pydantic import BaseModel

from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients


class CloneAndSmoothRequest(BaseModel):
    focusWorkspace: str
    smoothDataIngredients: SmoothDataExcludingPeaksIngredients

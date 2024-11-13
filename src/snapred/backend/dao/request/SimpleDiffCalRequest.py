from typing import Dict

from pydantic import BaseModel

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients


class SimpleDiffCalRequest(BaseModel):
    ingredients: DiffractionCalibrationIngredients
    groceries: Dict[str, str]

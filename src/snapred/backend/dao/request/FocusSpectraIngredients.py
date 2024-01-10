from ast import In
from typing import Optional, Union

from pydantic import BaseModel

from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients as Ingredients
from snapred.meta.Config import Config


class FocusSpectraIngredients(BaseModel):
    InputWorkspace: str
    GroupingWorkspace: str
    Ingredients: Union[Ingredients, str]
    OutputWorkspace: str

    def __init__(self, **data):
        if isinstance(data["Ingredients"], Ingredients):
            data["Ingredients"] = data["Ingredients"].json()
        super().__init__(**data)

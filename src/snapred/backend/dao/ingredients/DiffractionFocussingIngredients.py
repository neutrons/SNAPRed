
from pydantic import BaseModel

class DiffractionFocussingIngredients(BaseModel):
    InputWorkspace: str
    GroupingWorkspace: str
    OutputWorkspace: str
    PreserveEvents: bool = True
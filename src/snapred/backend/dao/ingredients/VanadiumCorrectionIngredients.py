from pydantic import BaseModel


class VanadiumCorrectionIngredients(BaseModel):
    InputWorkspace: str
    BackgroundWorkspace: str
    Ingredients: str
    CalibrationSample: str
    OutputWorkspace: str

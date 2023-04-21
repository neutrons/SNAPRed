from pydantic import BaseModel
from typing import Optional

class DiffractionCalibrant(BaseModel):
    runNumber: str
    filename: str
    diffCalPath: str
    name: Optional[str]
    latticeParameters: Optional[str] # though it is a csv string of floats
    reference: Optional[str]
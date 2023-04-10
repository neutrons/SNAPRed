from typing import Optional

from pydantic import BaseModel


class DiffractionCalibrant(BaseModel):
    runNumber: str
    filename: str
    name: Optional[str]
    latticeParameters: Optional[str]  # though it is a csv string of floats
    reference: Optional[str]

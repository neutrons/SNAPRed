from typing import List

from pydantic import BaseModel


class FocusGroup(BaseModel):
    name: str
    FWHM: List[int]
    # these props apply to allgroups? TODO: Move up a level?
    nHst: int
    dBin: List[float]
    dMax: List[float]
    dMin: List[float]
    definition: str

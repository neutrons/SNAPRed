from typing import List
from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class FocusGroup:
    name: str
    FWHM: List[float]
    # these props apply to allgroups? TODO: Move up a level?
    nHst: int
    dBin: List[float]
    dMax: List[float]
    dMin: List[float]
    definition: List[int]
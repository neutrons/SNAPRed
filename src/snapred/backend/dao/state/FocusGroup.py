from typing import List
from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class FocusGroup:
    name: str
    nHst: int  #what is nHst? number of histograms?
    dBin: List[float]
    dMax: List[float]
    dMin: List[float]
    definition: List[int]
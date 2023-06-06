from typing import Tuple

from pydantic import BaseModel


class CrystallographicPeak(BaseModel):
    """Class to hold crystallographic parameters"""

    hkl: Tuple[int, int, int]
    dSpacing: float
    fSquared: float
    multiplicity: int

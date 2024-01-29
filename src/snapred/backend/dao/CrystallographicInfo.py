from typing import List, Tuple

import numpy as np
from pydantic import BaseModel

from snapred.backend.dao.CrystallographicPeak import CrystallographicPeak


class CrystallographicInfo(BaseModel):
    """Class to hold crystallographic parameters"""

    peaks: List[CrystallographicPeak] = []

    @property
    def hkl(self) -> List[Tuple[int, int, int]]:
        return [p.hkl for p in self.peaks]

    @property
    def dSpacing(self) -> List[float]:
        return [p.dSpacing for p in self.peaks]

    @property
    def fSquared(self) -> List[float]:
        return [p.fSquared for p in self.peaks]

    @property
    def multiplicities(self) -> List[int]:
        return [p.multiplicity for p in self.peaks]

    def __init__(
        self,
        hkl: List[Tuple[int, int, int]] = [],
        dSpacing: List[float] = [],
        fSquared: List[float] = [],
        multiplicities: List[int] = [],
        peaks=[],
    ):
        if peaks != []:
            super().__init__(peaks=peaks)
            return
        if peaks == []:
            super().__init__(peaks=np.zeros(len(hkl), dtype=CrystallographicPeak))
            return
        if len(fSquared) != len(hkl):
            raise ValueError("Structure factors and hkl required to have same length")
        if len(multiplicities) != len(hkl):
            raise ValueError("Multiplicities and hkl required to have same length")
        if len(dSpacing) != len(hkl):
            raise ValueError("Spacings and hkl required to have same length")
        peaks = [
            CrystallographicPeak(hkl=hh, dSpacing=dd, fSquared=ff, multiplicity=mm)
            for hh, dd, ff, mm in zip(hkl, dSpacing, fSquared, multiplicities)
        ]
        super().__init__(peaks=peaks)

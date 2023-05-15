from typing import List, Tuple

from pydantic import BaseModel, root_validator

# from mantid.geometry import PointGroup


class CrystallographicInfo(BaseModel):
    """Class to hold crystallographic parameters"""

    hkl: List[Tuple[int, int, int]]
    d: List[float]
    fSquared: List[float]
    multiplicities: List[int]

    @root_validator
    def validate_scalar_fields(cls, values):
        if len(values.get("fSquared")) != len(values.get("hkl")):
            raise ValueError("Structure factors and hkl required to have same length")
        if len(values.get("multiplicities")) != len(values.get("hkl")):
            raise ValueError("Multiplicities and hkl required to have same length")
        if len(values.get("d")) != len(values.get("hkl")):
            raise ValueError("Spacings and hkl required to have same length")
        return values

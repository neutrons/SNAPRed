from typing import List, Tuple, Any

from pydantic import BaseModel, root_validator

# from mantid.geometry import PointGroup

class CrystallographicInfo(BaseModel):
    """Class to hold crystallographic parameters"""
    hkl: List[Tuple[int, int, int]]
    d: float
    fSquared: List[float]
    multiplicities: List[int]
    
    @root_validator
    def validate_scalar_fields(cls, values):
        if len(values.get('fSquared')) != len(values.get('hkl')):
            raise ValueError
        if len(values.get('multiplicities')) != len(values.get('hkl')):
            raise ValueError
        return values

    
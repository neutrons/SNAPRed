from typing import List, Optional

from pydantic import BaseModel, root_validator


class Geometry(BaseModel):
    """Class to hold Geometry data for Calibrant Samples
    Shape: string must be 'Cylinder' or 'Sphere'
    Radius: single float (cm)
    Height: single float (cm) (not used for sphere)
    Center: list of three floats (cm)"""

    Shape: str
    Radius: float
    Height: Optional[float]
    Center: List[float]

    @root_validator(pre=True, allow_reuse=True)
    def validate_form(cls, v):
        Shape, Height = v.get("Shape").strip(), v.get("Height")
        if Shape != "Cylinder" and Shape != "Sphere":
            raise ValueError('form must be "Cylinder" or "Sphere"')
        if Shape == "Sphere" and Height is not None:
            v.set("total_height", None)
            raise Warning("Height is not used with a sphere")
        return v


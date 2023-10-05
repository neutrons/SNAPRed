from typing import List, Optional

from pydantic import BaseModel, root_validator


class Geometry(BaseModel):
    """Class to hold Geometry data for Calibrant Samples
    shape: string must be 'Cylinder' or 'Sphere'
    radius: single float (cm)
    height: single float (cm) (not used for sphere)
    center: list of three floats (cm)"""

    shape: str
    radius: float
    height: Optional[float]
    center: List[float]

    # @root_validator(pre=True, allow_reuse=True)
    # def validate_form(cls, v):
    #     shape, height = v.get("shape").strip(), v.get("height")
    #     if shape != "Cylinder" and shape != "Sphere":
    #         raise ValueError('shape must be "Cylinder" or "Sphere"')
    #     if shape == "Sphere" and height is not None:
    #         v.set("height", None)
    #         raise Warning("height is not used with a sphere")
    #     return v

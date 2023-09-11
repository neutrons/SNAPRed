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

    @root_validator(pre=True, allow_reuse=True)
    def validate_form(cls, v):
        form, total_height = v.get("form").strip(), v.get("total_height")
        if form != "cylinder" and form != "sphere":
            raise ValueError('form must be "cylinder" or "sphere"')
        if form == "sphere" and total_height is not None:
            v.set("total_height", None)
            raise Warning("total height is not used with a sphere")
        if form == "cylinder" and total_height is None:
            raise ValueError("cylinders must have a total height")
        return v

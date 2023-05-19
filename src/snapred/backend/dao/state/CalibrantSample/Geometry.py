from typing import Optional

from pydantic import BaseModel, root_validator


class Geometry(BaseModel):
    """Class to hold Geometry data for Calibrant Samples
    form: string must be 'cylinder' or 'sphere'
    radius: single float (cm)
    illuminated height: single float (cm)
    total height: single float (cm) (not used for sphere)"""

    form: str
    radius: float
    illuminated_height: float
    total_height: Optional[float] = None

    @root_validator(pre=True)
    def validate_form(cls, v):
        form, total_height = v.get("form").strip(), v.get("total_height")
        if form != "cylinder" and form != "sphere":
            raise ValueError('form must be "cylinder" or "sphere"')
        if form == "sphere" and total_height is not None:
            v.set("total_height", None)
            raise Warning("total height is not used with a sphere")
        return v

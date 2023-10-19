import json
from typing import Any, Dict, Optional, Tuple

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
    center: Tuple[float, float, float] = (0, 0, 0)
    axis: Tuple[float, float, float] = (0, 1, 0)

    def json(self) -> str:
        ans = {
            "Shape": self.shape,
            "Radius": self.radius,
            "Center": list(self.center),
        }
        if self.shape == "Cylinder":
            ans["Height"] = self.height
            ans["Axis"] = list(self.axis)
        return json.dumps(ans)

    @root_validator(pre=True, allow_reuse=True)
    def validate_form(cls, v):
        shape, height = v.get("shape").strip(), v.get("height")
        if shape != "Cylinder" and shape != "Sphere":
            raise ValueError('shape must be "Cylinder" or "Sphere"')
        elif shape == "Cylinder" and height is None:
            raise RuntimeError("height must be set in cylinder")
        elif shape == "Sphere" and height is not None:
            v["height"] = None
            raise Warning("height is not used with a sphere")
        return v

import json
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, model_validator


class Geometry(BaseModel):
    """Class to hold Geometry data for Calibrant Samples
    shape: string must be 'Cylinder' or 'Sphere'
    radius: single float (cm)
    height: single float (cm) (not used for sphere)
    center: list of three floats (cm)"""

    shape: str
    radius: float
    height: Optional[float] = None
    center: Tuple[float, float, float] = (0, 0, 0)
    axis: Tuple[float, float, float] = (0, 1, 0)

    def dict(self, **kwargs) -> Dict[str, Any]:  # noqa: A003, ARG002
        ans = {
            "shape": self.shape,
            "radius": self.radius,
            "center": list(self.center),
        }
        if self.shape == "Cylinder":
            ans["height"] = self.height
            ans["axis"] = list(self.axis)
        return ans

    def json(self, **kwargs) -> str:
        return json.dumps(self.dict(), **kwargs)

    @model_validator(mode="before")
    @classmethod
    def validate_form(cls, v: Any):
        if isinstance(v, dict):
            shape, height = v.get("shape", "BadForm"), v.get("height")
            if shape != "Cylinder" and shape != "Sphere":
                raise ValueError('shape must be "Cylinder" or "Sphere"')
            elif shape == "Cylinder" and height is None:
                raise RuntimeError("height must be set in cylinder")
            elif shape == "Sphere" and height is not None:
                del v["height"]
                raise Warning("height is not used with a sphere")
        return v

import numpy as np
from pydantic import BaseModel, Field, field_validator

from snapred.backend.dao.Limit import Limit


class PixelGroupingParameters(BaseModel):
    groupID: int

    #  IMPORTANT: `PixelGroupingParameters` for fully-masked subgroups should no longer be included in the `PixelGroup`.
    isMasked: bool = Field(default=False, deprecated=True, exclude=True)

    # the mean sample to pixel distance (metres) for this pixel grouping
    L2: float

    # the mean two-theta (two times the polar angle in radians) for this pixel grouping
    twoTheta: float = Field(ge=0.0, le=2.0 * np.pi)

    # the mean azimuth (radians) for this pixel grouping
    azimuth: float = Field(ge=-np.pi, le=np.pi)

    # the minimum and maximum diffraction-resolvable d-spacing (Angstrom) for pixel grouping
    dResolution: Limit[float]

    # the relative diffraction resolution (Angstrom) for pixel grouping
    dRelativeResolution: float = Field(gt=0.0)

    @field_validator("dResolution", mode="before")
    @classmethod
    def validate_resolution_type(cls, v):
        if isinstance(v, dict):
            minimum = v.get("minimum")
            if minimum <= 0.0:
                raise ValueError("any valid d-spacing must be > 0.0")
            v = Limit[float](**v)
        elif not isinstance(v, Limit[float]):
            # Coerce the Generic[T] type
            v = Limit[float](**v.dict())
        return v

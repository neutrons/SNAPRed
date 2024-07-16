from typing import List

from pydantic import BaseModel


class FocusParams(BaseModel):
    # TODO: To be reevaluated for EWM 4798
    L2: List[float]
    Polar: List[float]
    Azimuthal: List[float]

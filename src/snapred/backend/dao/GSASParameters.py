from typing import Tuple

from pydantic import BaseModel


class GSASParameters(BaseModel):
    alpha: float
    beta: Tuple[float, float]

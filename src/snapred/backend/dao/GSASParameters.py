from typing import Tuple

from pydantic import BaseModel


class GSASParameters(BaseModel):
    """Class for various GSAS parameters"""

    alpha: float
    beta: Tuple[float, float]

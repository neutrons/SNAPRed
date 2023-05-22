from pydantic import BaseModel


class GSASParameters(BaseModel):
    alpha: float
    beta_0: float
    beta_1: float

from typing import List

from pydantic import BaseModel, validator


class GSASParameters(BaseModel):
    alpha: float
    beta: List[float]

    @validator("beta")
    def validate_beta(cls, v):
        if len(v) != 2:
            raise ValueError("beta must be a list of length 2")
        return v

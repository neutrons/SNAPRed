import json
from typing import Any, Dict, Optional

from pydantic import BaseModel, field_validator, model_validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    packingFraction: float, [0.0, 1.0]
    massDensity: float (g/cm^3)
    chemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    packingFraction: Optional[float] = None
    massDensity: Optional[float] = None
    chemicalFormula: str

    def dict(self, **kwargs) -> Dict[str, Any]:  # noqa: A003, ARG002
        ans = {
            "chemicalFormula": self.chemicalFormula,
        }
        if self.packingFraction is not None:
            ans["packingFraction"] = self.packingFraction
        if self.massDensity is not None:
            ans["massDensity"] = self.massDensity
        return ans

    def json(self, **kwargs) -> str:
        return json.dumps(self.dict(), **kwargs)

    @field_validator("packingFraction", mode="before")
    @classmethod
    def validate_packingFraction(cls, v):
        if v is not None and (v < 0.0 or v > 1.0):
            raise ValueError("packingFraction must be a value in the range [0.0, 1.0]")
        return v

    @field_validator("massDensity", mode="before")
    @classmethod
    def validate_massDensity(cls, v):
        if v is not None and v < 0.0:
            raise ValueError("massDensity must be positive")
        return v

    @model_validator(mode="before")
    @classmethod
    def validate_correctPropertiesToFindDensity(cls, v: Any):
        if isinstance(v, dict):
            symbols = v.get("chemicalFormula").replace("-", " ").split()
            md, pf = v.get("massDensity"), v.get("packingFraction")
            # multi-element material must include at minimum the mass density
            if len(symbols) > 1 and md is None:
                raise ValueError("for multi-element materials, must include mass density")
            if len(symbols) == 1 and md is not None and pf is not None:
                del v["packingFraction"]
                raise Warning("can't specify both mass-density and packing fraction for single-element materials")
        return v

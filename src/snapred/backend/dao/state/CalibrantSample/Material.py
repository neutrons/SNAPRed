import json
from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator, validator

# ruff: noqa: ARG002


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    packingFraction: float, [0.,1.]
    massDensity: float (g/cm^3)
    chemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    packingFraction: Optional[float]
    massDensity: Optional[float]
    chemicalFormula: str

    def dict(  # noqa: A003
        self,
        include=[],
        exclude=[],
        by_alias=False,
        exclude_unset=False,
        exclude_defaults=False,
        exclude_none=False,
    ) -> Dict[str, Any]:
        ans = {
            "chemicalFormula": self.chemicalFormula,
        }
        if self.packingFraction is not None:
            ans["packingFraction"] = self.packingFraction
        if self.massDensity is not None:
            ans["massDensity"] = self.massDensity
        return ans

    def json(self) -> str:
        return json.dumps(self.dict())

    @validator("packingFraction", allow_reuse=True)
    def validate_packingFraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("packingFraction must be a value in the range [0, 1]")
        return v

    @validator("massDensity", allow_reuse=True)
    def validate_massDensity(cls, v):
        if v < 0:
            raise ValueError("massDensity must be positive")
        return v

    @root_validator(pre=True, allow_reuse=True)
    def validate_correctPropertiesToFindDensity(cls, v):
        symbols = v.get("chemicalFormula").replace("-", " ").split()
        md, pf = v.get("massDensity"), v.get("packingFraction")
        # multi-element material must include at minimum the mass density
        if len(symbols) > 1 and md is None:
            raise ValueError("for multi-element materials, must include mass density")
        if len(symbols) == 1 and md is not None and pf is not None:
            del v["packingFraction"]
            raise Warning("can't specify both mass-density and packing fraction for single-element materials")
        return v

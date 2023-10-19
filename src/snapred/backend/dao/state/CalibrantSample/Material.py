from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator, validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    packingFraction: float, [0.,1.]
    massDensity: float (g/cm^3)
    chemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    packingFraction: Optional[float]
    massDensity: Optional[float]
    chemicalFormula: str

    def json(self) -> str:
        ans = {
            "ChemicalFormula": self.chemicalFormula,
        }
        if self.packingFraction is not None:
            ans["PackingFraction"] = self.packingFraction
        if self.massDensity is not None:
            ans["MassDensity"] = self.massDensity
        return str(ans).replace("'", '"')

    @validator("packingFraction", allow_reuse=True)
    def validate_packingFraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("packingFraction must be a value in the range [0, 1]")
        return v

    @root_validator(pre=True, allow_reuse=True)
    def validate_massDensity(cls, v):
        symbols = v.get("chemicalFormula").replace("-", " ").split()
        md, pf = v.get("massDensity"), v.get("packingFraction")
        if len(symbols) > 1:
            if md is None or pf is None:
                raise ValueError("for multi-element materials, must include both mass density and packing fraction")
        elif len(symbols) == 1:
            if md is not None and pf is not None:
                del v["massDensity"]
                raise Warning("can't specify both mass-density and packing fraction for single-element materials")
        return v

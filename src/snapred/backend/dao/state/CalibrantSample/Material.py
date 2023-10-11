from typing import Any, Dict, Optional

from pydantic import BaseModel, root_validator, validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    packingFraction: float, [0.,1.]
    massDensity: float (g/cm^3)
    chemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    packingFraction: float
    massDensity: Optional[float]
    chemicalFormula: str

    @property
    def materialDictionary(self) -> Dict[str, Any]:
        ans = {
            "ChemicalFormula": self.chemicalFormula,
            "PackingFraction": self.packingFraction,
        }
        if len(self.chemicalFormula.split()) > 1:
            ans["MassDensity"] = self.massDensity
        return ans

    @validator("packingFraction", allow_reuse=True)
    def validate_packingFraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("packingFraction must be a value in the range [0, 1]")
        return v

    @root_validator(pre=True, allow_reuse=True)
    def validate_singleElement(cls, v):
        symbols, md = v.get("chemicalFormula").split(), v.get("massDensity")
        if (len(symbols) > 1) and md is None:
            raise ValueError("for multi-element materials, must include mass density")
        elif (len(symbols) == 1) and md is not None:
            v.set("massDensity", 0)
            raise Warning("can't specify mass density for single-element materials; ignored")
        return v

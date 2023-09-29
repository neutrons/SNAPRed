from pydantic import BaseModel, validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    packingFraction: float, [0.,1.]
    massDensity: float (g/cm^3)
    chemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    packingFraction: float
    massDensity: float
    chemicalFormula: str

    @validator("packingFraction", allow_reuse=True)
    def validate_packingFraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("packingFraction must be a value in the range [0, 1]")
        return v

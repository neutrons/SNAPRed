from pydantic import BaseModel, validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    PackingFraction: float, [0.,1.]
    MassDensity: float (g/cm^3)
    ChemicalFormula: string following mantid

            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    PackingFraction: float
    MassDensity: float
    ChemicalFormula: str

    @validator("PackingFraction", allow_reuse=True)
    def validate_PackingFraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("PackingFraction must be a value in the range [0, 1]")
        return v
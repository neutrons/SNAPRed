from pydantic import BaseModel, validator


class Material(BaseModel):
    """Class to hold Material information relevant to Calibrant Samples
    microstructure: str must be 'poly-crystal' or 'single-crystal'
    packing-fraction: float, [0.,1.]
    mass density: float (g/cm^3)
    chemical composition: string following mantid
            
            convention: https://docs.mantidproject.org/nightly/concepts/Materials.html#id3"""

    microstructure: str
    packing_fraction: float
    mass_density: float
    chemical_composition: str

    @validator("microstructure", allow_reuse=True)
    def validate_microstructure(cls, v):
        v = v.strip()
        if v != "poly-crystal" and v != "single-crystal":
            raise ValueError("microstructure must be 'poly-crystal' or 'single-crystal'")
        return v

    @validator("packing_fraction", allow_reuse=True)
    def validate_packing_fraction(cls, v):
        if v < 0 or v > 1:
            raise ValueError("packing fraction must be a value in the range [0, 1]")
        return v

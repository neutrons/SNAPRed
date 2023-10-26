from typing import Tuple

from pydantic import BaseModel, StrictStr, validate_arguments, validator


class Atom(BaseModel):
    """Class containing atomic paramaters used in Calibrant Samples
    atomic parameters:

        symbol: string of chemical symbol
        coordinates:

            x coordinate: float in range [-1, 1]
            y coordinate: float in range [-1, 1]
            z coordinate: float in range [-1, 1]

        site occupation factor: float in range [0, 1]
        adp: positive float, default is 0.01"""

    symbol: str
    coordinates: Tuple[float, float, float]
    siteOccupationFactor: float
    adp: float = 0.1

    @validate_arguments
    def __init__(self, *args: StrictStr, **kwargs):
        if args:
            print(args)
            scatter = args[0].split()
            super().__init__(
                symbol=scatter[0],
                coordinates=[float(x) for x in scatter[1:4]],
                siteOccupationFactor=float(scatter[4]),
                adp=float(scatter[5]),
            )
        else:
            super().__init__(**kwargs)

    @validator("coordinates", allow_reuse=True)
    def validate_atom_coordinates(cls, v):
        if not all(-1 <= val <= 1 for val in v):
            raise ValueError("atom coordinates (x, y, z) must be all in range [-1, 1]")
        return v

    @validator("siteOccupationFactor", allow_reuse=True)
    def validate_site_occupation_factor(cls, v):
        if v < 0 or v > 1:
            raise ValueError("Site occupation factor must be a value in range [0, 1]")
        return v

    @validator("adp", allow_reuse=True)
    def validate_adp(cls, v):
        if v < 0:
            raise ValueError("adp must be a positive value")
        return v

    @property
    def getString(self) -> str:
        atomicString = self.symbol
        atomicString += f" {self.coordinates[0]}"
        atomicString += f" {self.coordinates[1]}"
        atomicString += f" {self.coordinates[2]}"
        atomicString += f" {self.siteOccupationFactor}"
        atomicString += f" {self.adp}"
        return atomicString

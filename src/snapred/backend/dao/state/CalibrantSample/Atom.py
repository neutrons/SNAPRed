from typing import List, Optional

from pydantic import BaseModel, validator


class Atom(BaseModel):
    """Class containing atomic paramaters used in Calibrant Samples
    atomic parameters:

        atom type: string of chemical symbol
        atom coordinates:

            x coordinate: float in range [-1, 1]
            y coordinate: float in range [-1, 1]
            z coordinate: float in range [-1, 1]

        site occupation factor: float in range [0, 1]
        adp: positive float, default is 0.01"""

    atom_type: str
    atom_coordinates: List[float]
    site_occupation_factor: float
    adp: float = 0.1

    @validator("atom_coordinates", allow_reuse=True)
    def validate_atom_coordinates(cls, v):
        if not all(-1 <= val <= 1 for val in v) and not len(v) == 3:
            raise ValueError("atom coordinates must be 3 values (x, y, z) all in range [-1, 1]")
        return v

    @validator("site_occupation_factor", allow_reuse=True)
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
        atomicString = self.atom_type
        atomicString += f" {self.atom_coordinates[0]}"
        atomicString += f" {self.atom_coordinates[1]}"
        atomicString += f" {self.atom_coordinates[2]}"
        atomicString += f" {self.site_occupation_factor}"
        atomicString += f" {self.adp}"
        return atomicString

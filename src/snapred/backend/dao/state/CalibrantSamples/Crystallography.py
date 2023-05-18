import os
from typing import List

from pydantic import BaseModel, validator


class Crystallography(BaseModel):
    """Class containing Crystallography paramaters used in Calibrant Samples
    cif file: string, full path to .cif file containing crystallographic properties
    space group: string, ITOC symbol for the space group
    lattice parameters: List of 6 floats: a, b, c, alpha, beta, gamma
    atomic parameters:
        atom type: string of chemical symbol
        atom coordinates:
            x coordinate: float in range [-1, 1]
            y coordinate: float in range [-1, 1]
            z coordinate: float in range [-1, 1]
        site occupation factor: float in range [0, 1]
        adp: positive float, default is 0.01"""

    cif_file: str
    space_group: str
    lattice_parameters: List[float]
    atom_type: str
    atom_coordinates = List[float]
    site_occupation_factor: float
    adp: float

    @validator("cif_file")
    def validate_cif_file(cls, v):
        if not os.path.exists(v):
            raise ValueError("cif file must be full path to a valid cif file")
        return v

    @validator("lattice_parameters")
    def validate_lattice_parameters(cls, v):
        if len(v) != 6:
            raise ValueError("lattice parameters must be a list of 6 floats: a, b, c, alpha, beta, gamma")
        return v

    @validator("atom_coordinates")
    def validate_atom_coordinates(cls, v):
        if not all(-1 <= val <= 1 for val in v) and not len(v) == 3:
            raise ValueError("atom coordinates must be 3 values (x, y, z) all in range [-1, 1]")
        return v

    @validator("site_occupation_factor")
    def validate_site_occupation_factor(cls, v):
        if v < 0 or v > 1:
            raise ValueError("Site occupation factor must be a value in range [0, 1]")
        return v

    @validator("adp")
    def validate_adp(cls, v):
        if v < 0:
            raise ValueError("adp must be a positive value")

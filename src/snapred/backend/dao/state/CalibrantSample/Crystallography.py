import os
from typing import List, Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.state.CalibrantSample.Atom import Atom


class Crystallography(BaseModel):
    """Class containing Crystallography paramaters used in Calibrant Samples
    cif file: string, full path to .cif file containing crystallographic properties
    space group: string, ITOC symbol for the space group
    lattice parameters: List of 6 floats: a, b, c, alpha, beta, gamma
    atoms: list of atomic parameters for each atom"""

    cif_file: str
    space_group: str
    lattice_parameters: List[float]
    atoms: List[Atom]

    @property
    def spaceGroupString(self) -> str:
        return ";".join([f"{atom.getString}" for atom in self.atoms])

    @validator("cif_file", allow_reuse=True)
    def validate_cif_file(cls, v):
        if not os.path.exists(v):
            raise ValueError("cif file must be full path to a valid cif file")
        if not v.endswith(".cif"):
            raise ValueError("cif_file must be a file with .cif extension")
        return v

    @validator("lattice_parameters", allow_reuse=True)
    def validate_lattice_parameters(cls, v):
        if len(v) != 6:
            raise ValueError("lattice parameters must be a list of 6 floats: a, b, c, alpha, beta, gamma")
        return v

import os
from typing import List

from mantid.geometry import SpaceGroupFactory
from pydantic import BaseModel, validator

from snapred.backend.dao.state.CalibrantSample.Atom import Atom


class Crystallography(BaseModel):
    """Class containing Crystallography paramaters used in Calibrant Samples
    cif file: string, full path to .cif file containing crystallographic properties
    space group: string, ITOC symbol for the space group
    lattice parameters: List of 6 floats: a, b, c, alpha, beta, gamma
    atoms: list of atomic parameters for each atom"""

    cifFile: str
    spaceGroup: str
    latticeParameters: List[float]
    atoms: List[Atom]

    @property
    def scattererString(self) -> str:
        return ";".join([f"{atom.getString}" for atom in self.atoms])

    @property
    def unitCellString(self) -> str:
        return " ".join([f"{param}" for param in self.latticeParameters])

    @property
    def spaceGroupString(self) -> str:
        return self.spaceGroup

    @validator("cifFile", allow_reuse=True)
    def validate_cifFile(cls, v):
        if not os.path.exists(v):
            raise ValueError("cif file must be full path to a valid cif file")
        if not v.endswith(".cif"):
            raise ValueError("cif_file must be a file with .cif extension")
        return v

    @validator("latticeParameters", allow_reuse=True)
    def validate_latticeParameters(cls, v):
        if len(v) != 6:
            raise ValueError("lattice parameters must be a list of 6 floats: a, b, c, alpha, beta, gamma")
        return v

    @validator("spaceGroup", allow_reuse=True)
    def validate_spaceGroup(cls, v):
        if v not in SpaceGroupFactory.getAllSpaceGroupSymbols():
            raise ValueError("given space group does not exist")
        return v

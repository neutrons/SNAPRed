import os
from pathlib import Path
from typing import List, Tuple

from mantid.geometry import CrystalStructure, SpaceGroupFactory
from pydantic import BaseModel, field_validator, validate_call

from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.meta.Config import Config


class Crystallography(BaseModel):
    """Class containing Crystallography paramaters used in Calibrant Samples
    cif file: string, full path to .cif file containing crystallographic properties
    space group: string, ITOC symbol for the space group
    lattice parameters: Tuple of 6 floats: a, b, c, alpha, beta, gamma
    atoms: list of atomic parameters for each atom"""

    @classmethod
    def calibrationSampleHome(cls) -> Path:
        return Path(Config["instrument.calibration.sample.home"])

    cifFile: str
    spaceGroup: str
    latticeParameters: Tuple[float, float, float, float, float, float]
    atoms: List[Atom]

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def __init__(self, *args: str | CrystalStructure, **kwargs):
        if args:
            cifFile: str = args[0]
            xtal: CrystalStructure = args[1]
            unitCell = xtal.getUnitCell()
            super().__init__(
                cifFile=cifFile,
                spaceGroup=str(xtal.getSpaceGroup().getHMSymbol()),
                latticeParameters=[getattr(unitCell, x)() for x in ("a", "b", "c", "alpha", "beta", "gamma")],
                atoms=[Atom(scatterer) for scatterer in xtal.getScatterers()],
            )
        else:
            super().__init__(**kwargs)

    @property
    def scattererString(self) -> str:
        return ";".join([f"{atom.getString}" for atom in self.atoms])

    @property
    def unitCellString(self) -> str:
        return " ".join([f"{param}" for param in self.latticeParameters])

    @property
    def spaceGroupString(self) -> str:
        return self.spaceGroup

    @field_validator("cifFile")
    @classmethod
    def validate_cifFile(cls, v):
        filePath = Path(v)
        if not filePath.is_absolute():
            filePath = cls.calibrationSampleHome().joinpath(filePath)
        if not os.path.exists(filePath):
            raise ValueError(
                "'cifFile' must either be a relative path in 'calibration.sample.home'"
                + " or the full path to a valid cif file"
            )
        if not v.endswith(".cif"):
            raise ValueError("'cifFile' must be a file with .cif extension")
        return v

    @field_validator("spaceGroup")
    @classmethod
    def validate_spaceGroup(cls, v):
        if v not in SpaceGroupFactory.getAllSpaceGroupSymbols():
            raise ValueError("given space group does not exist")
        return v

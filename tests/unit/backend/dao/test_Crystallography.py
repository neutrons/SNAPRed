import unittest

import pytest
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.meta.Config import Resource


class TestCrystallography(unittest.TestCase):
    def setUp(self):
        atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
        self.crystal = Crystallography(
            cifFile=Resource.getPath("/inputs/crystalInfo/example.cif"),
            spaceGroup="F d -3 m",
            latticeParameters=[5.43159, 5.43159, 5.43159, 90.0, 90.0, 90.0],
            atoms=[atom, atom, atom],
        )

    def test_scattererString(self):
        assert (
            self.crystal.scattererString
            == "Si 0.125 0.125 0.125 1.0 0.1;Si 0.125 0.125 0.125 1.0 0.1;Si 0.125 0.125 0.125 1.0 0.1"
        )

    def test_unitCellString(self):
        assert self.crystal.unitCellString == "5.43159 5.43159 5.43159 90.0 90.0 90.0"

    def test_spaceGroupString(self):
        assert self.crystal.spaceGroupString == "F d -3 m"

    def test_SixLatticeParams(self):
        for i in range(1, 5):
            with pytest.raises(Exception):  # noqa: PT011
                Crystallography(
                    cifFile=Resource.getPath("/inputs/crystalInfo/example.cif"),
                    spaceGroup="BCC",
                    latticeParameters=range(1, i + 1),
                    atoms=[Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)],
                )
        with pytest.raises(Exception):  # noqa: PT011
            Crystallography(
                cifFile=Resource.getPath("/inputs/crystalInfo/example.cif"),
                spaceGroup="BCC",
                latticeParameters=range(6),
                atoms=[Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)],
            )

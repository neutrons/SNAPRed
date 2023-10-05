import unittest

from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography


class TestCrystallography(unittest.TestCase):
    def setUp(self):
        atom = Atom(atom_type="Si", atom_coordinates=[0.125, 0.125, 0.125], site_occupation_factor=1.0)
        self.crystal = Crystallography(
            cifFile="tests/resources/inputs/crystalInfo/example.cif",
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

import pytest
from pydantic import ValidationError
from snapred.backend.dao.state.CalibrantSample.Atom import Atom


def testGetString():
    atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
    assert atom.getString == "Si 0.125 0.125 0.125 1.0 0.1"


def testInitFromString():
    atom1 = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
    atom2 = Atom(atom1.getString)
    assert atom1 == atom2


def testThreeCoordinates():
    with pytest.raises(Exception):  # noqa: PT011
        Atom(symbol="V", coordinates=[1.0], siteOccupationFactor=1.0)
    with pytest.raises(Exception):  # noqa: PT011
        Atom(symbol="V", coordinates=[1.0, 2.0, 3.0, 4.0], siteOccupationFactor=1.0)


def testInvalidString():
    with pytest.raises(ValidationError):
        Atom(1)

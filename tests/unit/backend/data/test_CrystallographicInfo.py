import sys
import unittest.mock as mock

import pytest

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo  # noqa: E402

def setup():
    """Setup before all tests"""
    pass


def teardown():
    """Teardown after all tests"""
    pass



@pytest.fixture(autouse=True)
def setup_teardown():  # noqa: PT004
    """Setup before each test, teardown after each test"""
    setup()
    yield
    teardown()


def create_mock_pointgroup(v):
    pg = mock.Mock()
    pg.getEquivalents.return_value = v
    return pg

def create_inputs(v):
    hkl = [ (1,0,0), (1,1,1), (0,1,0)]
    fSquared = [9.0, 16.0, 25.0]
    mock_pg_equivs = [1]*len(hkl)
    d = 1e-5
    pg = create_mock_pointgroup(v)
    multiplicities = pg.getEquivalents(hkl)
    return hkl, d, fSquared, multiplicities

def test_create():
    """Test of Crystallographic Info DAO"""

    mock_pg_equivs = [1]*3
    hkl, d, fSquared, multiplicities = create_inputs(mock_pg_equivs)

    crystalInfo = CrystallographicInfo(
        hkl=hkl, 
        d=d, 
        fSquared=fSquared,
        multiplicities = multiplicities
    )

    assert crystalInfo is not None
    assert mock_pg_equivs == crystalInfo.multiplicities
    assert len(crystalInfo.hkl) == len(crystalInfo.fSquared)
    assert len(crystalInfo.hkl) == len(crystalInfo.multiplicities)

def test_failed_create():
    """Test of Failing Crystallographic DAO"""

    # there is an extra point in hkl, with no fSqaured or multiplicity
    mock_pg_equivs = [1]*3
    hkl, d, fSquared, multiplicities = create_inputs(mock_pg_equivs)
    hkl = [ (1,0,0), (1,1,1), (0,1,0,), (0,0,1)] 

    try:
        crystalInfo = CrystallographicInfo(
            hkl=hkl, 
            d=d, 
            fSquared=fSquared,
            multiplicities = multiplicities
        )
    except:
        assertTrue()
    else:
        pytest.fail("Should have failed to validate CrystallographicInfo")



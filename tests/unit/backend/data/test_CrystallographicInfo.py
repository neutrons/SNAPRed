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

def test_create():
    """Test of Crystallographic Info DAO"""

    hkl = [ (1,0,0), (1,1,1), (0,1,0)]
    fSquared = [9.0, 16.0, 25.0]
    mock_pg_equivs = [1]*len(hkl)
    d = 1e-5
    pg = create_mock_pointgroup(mock_pg_equivs)
    multiplicities = pg.getEquivalents(hkl)

    crystalInfo = CrystallographicInfo(
        hkl=hkl, 
        d=d, 
        fSquared=fSquared,
        multiplicities = multiplicities
    )

    assert crystalInfo is not None
    assert hkl == crystalInfo.hkl
    assert d == crystalInfo.d
    assert fSquared == crystalInfo.fSquared
    assert mock_pg_equivs == crystalInfo.multiplicities
    assert len(hkl) == len(fSquared)
    assert len(hkl) == len(crystalInfo.multiplicities)

def test_failed_create():
    """Test of Failing Crystallographic DAO"""

    # there is an extra point in hkl, with no fSqaured or multiplicity
    hkl = [ (1,0,0), (1,1,1), (0,1,0,), (0,0,1)] 
    fSquared = [9.0, 16.0, 25.0]
    mock_pg_equivs = [1]*len(hkl)
    d = 1e-5
    pg = create_mock_pointgroup(mock_pg_equivs)
    multiplicities = pg.getEquivalents(hkl)

    try:
        crystalInfo = CrystallographicInfo(
            hkl=hkl, 
            d=d, 
            fSquared=fSquared,
            multiplicities = multiplicities
        )
    except:
        assert True
    else:
        pytest.fail("Should have failed to validate CrystallographicInfo")



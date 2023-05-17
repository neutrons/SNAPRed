import unittest.mock as mock

import pytest
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo  # noqa: E402

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):

    def create_mock_pointgroup(mock_pg_equivs):
        pg = mock.Mock()
        pg.getEquivalents.return_value = mock_pg_equivs
        return pg

    def create_inputs(mock_pg_equivs=[]):
        hkl = [(1, 0, 0), (1, 1, 1), (0, 1, 0)]
        fSquared = [9.0, 16.0, 25.0]
        if mock_pg_equivs == []:
            mock_pg_equivs = [1] * len(hkl)
        d = [1e-5] * len(hkl)
        pg = create_mock_pointgroup(mock_pg_equivs)
        multiplicities = pg.getEquivalents(hkl)
        return hkl, d, fSquared, multiplicities

    def test_create():
        """Test of Crystallographic Info DAO"""

        mock_pg_equivs = [1] * 3
        hkl, d, fSquared, multiplicities = create_inputs(mock_pg_equivs)

        crystalInfo = CrystallographicInfo(hkl=hkl, d=d, fSquared=fSquared, multiplicities=multiplicities)

        assert crystalInfo is not None
        assert mock_pg_equivs == crystalInfo.multiplicities
        assert len(crystalInfo.hkl) == len(crystalInfo.fSquared)
        assert len(crystalInfo.hkl) == len(crystalInfo.multiplicities)
        assert len(crystalInfo.hkl) == len(crystalInfo.d)

    def test_failed_create():
        """Test of Failing Crystallographic DAO"""

        # there is an extra point in hkl, with no fSqaured or multiplicity
        hkl, d, fSquared, multiplicities = create_inputs()
        hkl.append((0, 0, 1))

        with pytest.raises(ValueError, match=r".* hkl .*"):
            CrystallographicInfo(hkl=hkl, d=d, fSquared=fSquared, multiplicities=multiplicities)

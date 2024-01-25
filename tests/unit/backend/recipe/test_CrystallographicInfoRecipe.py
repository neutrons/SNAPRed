import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo  # noqa : E402
    from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe as Recipe  # noqa: E402
    from snapred.meta.Config import Resource

    def test_good_path():
        """Test success of crystal ingestion recipe with a good path name"""
        goodCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        try:
            xtalRecipe = Recipe()
            data = xtalRecipe.executeRecipe(goodCIF, 1.0, 10.0)
            xtal = data["crystalInfo"]
        except:  # noqa: BLE011 E722
            pytest.fail("valid file failed to open")
        else:
            assert isinstance(xtal, CrystallographicInfo)
            assert xtal.hkl[0] == (1, 1, 1)
            assert xtal.hkl[5] == (4, 0, 0)
            assert xtal.dSpacing[0] == 3.13592994862768
            assert xtal.dSpacing[4] == 1.0453099828758932

    def test_failed_path():
        """Test failure of crystal ingestion recipe with a bad path name"""
        fakeCIF = "fake_file.cif"
        xtalRecipe = Recipe()
        with pytest.raises(Exception):  # noqa: PT011
            xtalRecipe.executeRecipe(fakeCIF)

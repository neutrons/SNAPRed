import json
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.recipe.algorithm.CrystallographicInfoAlgorithm import (
        CrystallographicInfoAlgorithm as Algo,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    def test_init_path():
        """Test ability to initialize crystal ingestion algo from path name"""
        fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")
        xtalAlgo = Algo()
        xtalAlgo.initialize()
        xtalAlgo.setProperty("CifPath", fakeCIF)
        assert fakeCIF == xtalAlgo.getProperty("CifPath").value

    def test_failed_path():
        """Test failure of crystal ingestion algo with a bad path name"""
        fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")
        xtalAlgo = Algo()
        xtalAlgo.initialize()
        xtalAlgo.setProperty("CifPath", fakeCIF)
        with pytest.raises(Exception):  # noqa: PT011
            xtalAlgo.execute()

    def test_good_path():
        """Test success of crystal ingestion algo with a good path name"""
        goodCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        try:
            xtalAlgo = Algo()
            xtalAlgo.initialize()
            xtalAlgo.setProperty("CifPath", goodCIF)
            xtalAlgo.setProperty("crystalDMin", 1.0)
            xtalAlgo.setProperty("crystalDMax", 10.0)
            xtalAlgo.execute()
            xtalInfo = json.loads(xtalAlgo.getPropertyValue("CrystalInfo"))
            xtallography = json.loads(xtalAlgo.getPropertyValue("Crystallography"))
        except Exception:  # noqa: BLE001
            pytest.fail("valid file failed to open")
        else:
            assert xtalInfo["peaks"][0]["hkl"] == [1, 1, 1]
            assert xtalInfo["peaks"][5]["hkl"] == [4, 0, 0]
            assert xtalInfo["peaks"][0]["dSpacing"] == 3.13592994862768
            assert xtalInfo["peaks"][4]["dSpacing"] == 1.0453099828758932
            assert xtallography["cifFile"] == goodCIF
            assert xtallography["spaceGroup"] == "F d -3 m"

    def test_from_xtal():
        """Test success of crystal ingestion algo using Crystallography object"""
        from snapred.backend.dao.state.CalibrantSample.Atom import Atom
        from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography

        goodCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
        xtallography = Crystallography(
            cifFile=goodCIF,
            spaceGroup="F d -3 m",
            latticeParameters=[5.43159, 5.43159, 5.43159, 90.0, 90.0, 90.0],
            atoms=[atom, atom, atom],
        )
        try:
            xtalAlgo = Algo()
            xtalAlgo.initialize()
            xtalAlgo.setProperty("Crystallography", xtallography.json())
            xtalAlgo.setProperty("crystalDMin", 1.0)
            xtalAlgo.setProperty("crystalDMax", 10.0)
            xtalAlgo.execute()
            xtalInfo = json.loads(xtalAlgo.getPropertyValue("CrystalInfo"))
            xtallography2 = json.loads(xtalAlgo.getPropertyValue("Crystallography"))
        except Exception:  # noqa: BLE001
            pytest.fail("valid file failed to open")
        else:
            assert xtalInfo["peaks"][0]["hkl"] == [1, 1, 1]
            assert xtalInfo["peaks"][5]["hkl"] == [4, 0, 0]
            assert xtalInfo["peaks"][0]["dSpacing"] == 3.13592994862768
            assert xtalInfo["peaks"][4]["dSpacing"] == 1.0453099828758932
            assert xtallography2["cifFile"] == goodCIF
            assert xtallography2["spaceGroup"] == "F d -3 m"

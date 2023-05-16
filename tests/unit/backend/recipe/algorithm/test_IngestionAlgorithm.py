import json

import pytest
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
    IngestCrystallographicInfoAlgorithm,  # noqa: E402
)
from snapred.meta.Config import Resource


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


def test_init_path():
    """Test ability to initialize crystal ingestion algo from path name"""
    fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")
    ingestAlgo = IngestCrystallographicInfoAlgorithm()
    ingestAlgo.initialize()
    ingestAlgo.setProperty("cifPath", fakeCIF)
    assert fakeCIF == ingestAlgo.getProperty("cifPath").value


def test_failed_path():
    """Test failure of crystal ingestion algo with a bad path name"""
    fakeCIF = Resource.getPath("/inputs/crystalInfo/fake_file.cif")
    ingestAlgo = IngestCrystallographicInfoAlgorithm()
    ingestAlgo.initialize()
    ingestAlgo.setProperty("cifPath", fakeCIF)
    with pytest.raises(Exception):  # noqa: PT011
        ingestAlgo.execute()


def test_good_path():
    """Test success of crystal ingestion algo with a good path name"""
    goodCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
    try:
        ingestAlgo = IngestCrystallographicInfoAlgorithm()
        ingestAlgo.initialize()
        ingestAlgo.setProperty("cifPath", goodCIF)
        ingestAlgo.execute()
        xtalInfo = json.loads(ingestAlgo.getProperty("crystalInfo").value)
    except Exception:  # noqa: BLE001
        pytest.fail("valid file failed to open")
    else:
        assert xtalInfo["hkl"][0] == [1, 1, 1]
        assert xtalInfo["hkl"][5] == [4, 0, 0]
        assert xtalInfo["d"][0] == 3.13592994862768
        assert xtalInfo["d"][4] == 1.0453099828758932

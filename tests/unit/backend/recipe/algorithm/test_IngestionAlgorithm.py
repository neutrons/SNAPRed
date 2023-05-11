import sys
import unittest.mock as mock

import pytest

from snapred.backend.recipe.algorithm.IngestCrystallographicInfo import IngestCrystallographicInfo  # noqa: E402

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


def test_weak_f_squared_lowest():
    """Test weak fSquared method finds lowest 1%"""
    ingestAlgo = IngestCrystallographicInfo()
    assert not isinstance(ingestAlgo, mock.Mock)
    mock_xtal = mock.Mock()
    mock_xtal.fSquared = list(range(36, 136))
    mock_xtal.multiplicities = [1,2,3,4,5]*20

    assert len(mock_xtal.fSquared) == len(mock_xtal.multiplicities)

    assert ingestAlgo.findWeakFSquared(mock_xtal) == 36

def test_weak_f_squared_median():
    """Test weak fSquared method finds median of lowest 1%"""
    ingestAlgo = IngestCrystallographicInfo()
    assert not isinstance(ingestAlgo, mock.Mock)
    mock_xtal = mock.Mock()
    mock_xtal.fSquared = list(range(36, 336))
    mock_xtal.multiplicities = [1,2,3,4,5]*60

    assert len(mock_xtal.fSquared) == len(mock_xtal.multiplicities)

    assert ingestAlgo.findWeakFSquared(mock_xtal) != 36
    assert ingestAlgo.findWeakFSquared(mock_xtal) == 41

def test_weak_f_squared_small():
    """Test weak fSquared method finds an answer with small number of options"""
    ingestAlgo = IngestCrystallographicInfo()
    assert not isinstance(ingestAlgo, mock.Mock)
    mock_xtal = mock.Mock()
    mock_xtal.fSquared = [2, 3, 4, 5]
    mock_xtal.multiplicities = [1, 1, 1, 1]

    assert ingestAlgo.findWeakFSquared(mock_xtal) == 2


def test_weak_f_squared_small():
    """Test weak fSquared method finds an answer with only one option"""
    ingestAlgo = IngestCrystallographicInfo()
    assert not isinstance(ingestAlgo, mock.Mock)
    mock_xtal = mock.Mock()
    mock_xtal.fSquared = [2]
    mock_xtal.multiplicities = [1]

    assert ingestAlgo.findWeakFSquared(mock_xtal) == 2




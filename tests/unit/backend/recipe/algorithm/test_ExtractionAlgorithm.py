import unittest.mock as mock

import pytest

mock.patch("mantid.api")

from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402


def teardown():
    """Teardown after all tests"""
    mock.patch.stopall()


@pytest.fixture(autouse=True)
def setup_teardown():  # noqa: PT004
    """Setup before each test, teardown after each test"""
    yield
    teardown()


def test_execute():
    """Test exection of ExtractionAlgorithm"""
    extractionAlgo = ExtractionAlgorithm()
    extractionAlgo.execute()

    print(extractionAlgo.log().notice.call_args)
    assert extractionAlgo is not None
    assert extractionAlgo.log.called
    assert extractionAlgo.ExtractionIngredients is not None

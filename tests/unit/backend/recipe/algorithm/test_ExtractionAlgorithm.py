import sys
import unittest.mock as mock

import pytest

from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402
from snapred.backend.recipe.algorithm.DummyAlgo import DummyAlgo  # noqa: E402


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


def test_execute():
    # """Test exection of ExtractionAlgorithm"""
    # extractIngredients = mock.Mock()
    # extractionAlgo = ExtractionAlgorithm(ExtractionIngredients = extractIngredients)
    # # extractionAlgo.execute()

    # print(extractionAlgo.log().notice.call_args)
    # assert extractionAlgo is not None
    # assert extractionAlgo.log.called
    # assert extractionAlgo.ExtractionIngredients is not None
    pass


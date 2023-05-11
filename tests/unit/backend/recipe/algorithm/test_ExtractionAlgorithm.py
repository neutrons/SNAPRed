import pytest
from snapred.backend.recipe.algorithm.ExtractionAlgorithm import ExtractionAlgorithm  # noqa: E402


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
    """Test exection of ExtractionAlgorithm"""
    extractionAlgo = ExtractionAlgorithm()
    extractionAlgo.initialize()
    extractionAlgo.execute()
    assert True

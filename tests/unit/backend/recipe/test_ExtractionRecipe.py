import sys
import unittest.mock as mock

import pytest

# Mock out of scope modules before importing ExtractionRecipe
sys.modules["mantid.api"] = mock.Mock()

# mock an AlgorithmManager to return a mocked algorithm
mockAlgo = mock.Mock()
AlgorithmManager = mock.Mock()
AlgorithmManager.create.return_value = mockAlgo

from snapred.backend.recipe.ExtractionRecipe import ExtractionRecipe  # noqa: E402


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


@mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", AlgorithmManager)
def test_execute_successful():
    mockAlgo.execute.return_value = "passed"

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()
    data = recipe.executeRecipe(ingredients)

    assert AlgorithmManager.create.called
    assert mockAlgo.execute.called
    assert isinstance(data, dict)
    assert data["result"] is not None
    assert data["result"] == "passed"


@mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", AlgorithmManager)
def test_execute_unsuccessful():
    mockAlgo.execute.side_effect = (RuntimeError(),)

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()
    try:
        recipe.executeRecipe(ingredients)
    except:  # noqa: E722
        assert AlgorithmManager.create.called
        assert mockAlgo.execute.called
    else:
        # fail if execute did not raise an exception
        pytest.fail("Failure to fail")

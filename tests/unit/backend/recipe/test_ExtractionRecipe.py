import unittest.mock as mock

import pytest
from snapred.backend.recipe.ExtractionRecipe import ExtractionRecipe

# mock an AlgorithmManager to return a mocked algorithm
mockAlgo = mock.Mock()
MockAlgorithmManager = mock.Mock()
MockAlgorithmManager.create.return_value = mockAlgo

@mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", MockAlgorithmManager)
def test_execute_successful():
    mockAlgo.execute.return_value = "passed"
    mockAlgo.ingredients = ""
    mockAlgo.setProperty.side_effect = lambda x, y: setattr(mockAlgo, "ingredients", y)  # noqa: ARG005

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()
    ingredients.json = mock.Mock(return_value="good json")
    data = recipe.executeRecipe(ingredients)

    assert MockAlgorithmManager.create.called
    assert mockAlgo.execute.called
    assert ingredients.json.called
    assert mockAlgo.setProperty.called
    assert mockAlgo.ingredients == "good json"
    assert isinstance(data, dict)
    assert data["result"] is not None
    assert data["result"] == "passed"


@mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", MockAlgorithmManager)
def test_execute_unsuccessful():
    mockAlgo.execute.side_effect = RuntimeError("passed")

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()

    try:
        recipe.executeRecipe(ingredients)
    except Exception as e:  # noqa: E722 BLE001
        assert str(e) == "passed"  # noqa: PT017
        assert MockAlgorithmManager.create.called
        assert mockAlgo.execute.called
    else:
        # fail if execute did not raise an exception
        pytest.fail("Test should have raised RuntimeError, but no error raised")

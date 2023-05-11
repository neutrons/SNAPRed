import unittest.mock as mock

import pytest

# Mock out of scope modules before importing ExtractionRecipe
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock()}):
    # mock an AlgorithmManager to return a mocked algorithm
    mockAlgo = mock.Mock()
    AlgorithmManager = mock.Mock()
    AlgorithmManager.create.return_value = mockAlgo

    from snapred.backend.recipe.ExtractionRecipe import ExtractionRecipe  # noqa: E402

    @mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", AlgorithmManager)
    def test_execute_successful():
        mockAlgo.execute.return_value = "passed"
        mockAlgo.ingredients = ""
        mockAlgo.setProperty.side_effect = lambda x, y: setattr(mockAlgo, "ingredients", y)  # noqa: ARG005

        recipe = ExtractionRecipe()
        ingredients = mock.Mock()
        ingredients.json = mock.Mock(return_value="good json")
        data = recipe.executeRecipe(ingredients)

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
    mockAlgo.ingredients = ""
    mockAlgo.setProperty.side_effect = lambda x, y: setattr(mockAlgo, "ingredients", y)  # noqa: ARG005

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()
    ingredients.json = mock.Mock(return_value="good json")
    data = recipe.executeRecipe(ingredients)

    assert AlgorithmManager.create.called
    assert mockAlgo.execute.called
    assert ingredients.json.called
    assert mockAlgo.setProperty.called
    assert mockAlgo.ingredients == "good json"
    assert isinstance(data, dict)
    assert data["result"] is not None
    assert data["result"] == "passed"


@mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", AlgorithmManager)
def test_execute_unsuccessful():
    mockAlgo.execute.side_effect = RuntimeError()

    recipe = ExtractionRecipe()
    ingredients = mock.Mock()

    try:
        recipe.executeRecipe(ingredients)
    except:  # noqa: E722
        assert AlgorithmManager.create.called
        assert mockAlgo.execute.called
        assert ingredients.json.called
        assert mockAlgo.setProperty.called
        assert mockAlgo.ingredients == "good json"
        assert isinstance(data, dict)
        assert data["result"] is not None
        assert data["result"] == "passed"

    @mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager", AlgorithmManager)
    def test_execute_unsuccessful():
        mockAlgo.execute.side_effect = RuntimeError()

        recipe = ExtractionRecipe()
        ingredients = mock.Mock()

        try:
            recipe.executeRecipe(ingredients)
        except:  # noqa: E722
            assert AlgorithmManager.create.called
            assert mockAlgo.execute.called
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should raised RuntimeError: no error raised")

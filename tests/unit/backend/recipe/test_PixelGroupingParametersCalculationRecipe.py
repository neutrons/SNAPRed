import json
import unittest.mock as mock

import pytest
from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.meta.redantic import list_to_raw


@mock.patch("snapred.backend.recipe.PixelGroupingParametersCalculationRecipe.BinnedValue")
def test_execute_successful(mockBinnedValue):
    # mock algorithm execution result and output
    params = PixelGroupingParameters(
        groupID=1,
        isMasked=False,
        twoTheta=3.14,
        dResolution=Limit(minimum=0.1, maximum=1.0),
        dRelativeResolution=0.01,
    )
    mock_output_val = [params]
    mockAlgo = mock.Mock(return_value=list_to_raw(mock_output_val))

    # execute recipe with mocked input
    recipe = PixelGroupingParametersCalculationRecipe()
    recipe.mantidSnapper.PixelGroupingParametersCalculationAlgorithm = mockAlgo
    ingredients = mock.Mock(json=mock.Mock(return_value="good ingredients"))
    ingredients.nBinsAcrossPeakWidth = 7
    groceries = {
        "groupingWorkspace": "grouping workspace",
        "maskWorkspace": "mask workspace",
    }
    data = recipe.executeRecipe(ingredients, groceries)

    assert mockAlgo.call_count == 1
    assert isinstance(data, dict)
    assert data["result"]
    assert isinstance(data["parameters"], list)
    assert data["parameters"][0] == mock_output_val[0]
    assert data["tof"] == mockBinnedValue.return_value


def test_execute_unsuccessful():
    mockAlgo = mock.Mock(side_effect=RuntimeError("passed"))

    recipe = PixelGroupingParametersCalculationRecipe()
    recipe.mantidSnapper.PixelGroupingParametersCalculationAlgorithm = mockAlgo
    ingredients = mock.Mock()
    groceries = {
        "groupingWorkspace": "grouping workspace",
        "maskWorkspace": "mask workspace",
    }

    with pytest.raises(RuntimeError) as e:
        recipe.executeRecipe(ingredients, groceries)
    assert str(e.value) == "passed"  # noqa: PT017
    assert mockAlgo.call_count == 1


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)

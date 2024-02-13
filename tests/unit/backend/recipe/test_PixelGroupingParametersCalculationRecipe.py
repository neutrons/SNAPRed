import json
import unittest.mock as mock

import pytest

with mock.patch("mantid.api.AlgorithmManager") as MockAlgorithmManager:
    from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
    from snapred.backend.dao.Limit import Limit
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
    from snapred.meta.redantic import list_to_raw

    mockAlgo = mock.Mock()
    MockAlgorithmManager.create.return_value = mockAlgo

    @mock.patch("snapred.backend.recipe.PixelGroupingParametersCalculationRecipe.BinnedValue")
    def test_execute_successful(mockBinnedValue):
        # mock algorithm execution result and output
        mockAlgo.execute.return_value = "passed"
        params = PixelGroupingParameters(
            groupID=1, isMasked=False, twoTheta=3.14, dResolution=Limit(minimum=0.1, maximum=1.0), dRelativeResolution=0.01
        )
        mock_output_val = [params]
        mockAlgo.getProperty("OutputParameters").value = list_to_raw(mock_output_val)

        # execute recipe with mocked input
        recipe = PixelGroupingParametersCalculationRecipe()
        ingredients = mock.Mock(return_value="good ingredients")
        ingredients.nBinsAcrossPeakWidth = 7
        groceries = {
            "groupingWorkspace": mock.Mock(return_value="grouping workspace"),
            "maskWorkspace": mock.Mock(return_value="mask workspace"),
        }
        data = recipe.executeRecipe(ingredients, groceries)

        assert mockAlgo.execute.called
        assert isinstance(data, dict)
        assert data["result"] is not None
        assert data["result"] == "passed"
        assert isinstance(data["parameters"], list)
        assert data["parameters"][0] == mock_output_val[0]
        assert data["tof"] == mockBinnedValue.return_value

    def test_execute_unsuccessful():
        mockAlgo.execute.side_effect = RuntimeError("passed")

        recipe = PixelGroupingParametersCalculationRecipe()
        ingredients = mock.Mock()
        groceries = {
            "groupingWorkspace": mock.Mock(return_value="grouping workspace"),
            "maskWorkspace": mock.Mock(return_value="mask workspace"),
        }

        try:
            recipe.executeRecipe(ingredients, groceries)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            assert mockAlgo.execute.called
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")


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

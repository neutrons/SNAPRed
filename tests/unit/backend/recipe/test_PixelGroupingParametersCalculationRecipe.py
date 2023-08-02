import json
import unittest.mock as mock

import pytest

with mock.patch("mantid.api.AlgorithmManager") as MockAlgorithmManager:
    from snapred.backend.dao.Limit import Limit
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe

    mockAlgo = mock.Mock()
    MockAlgorithmManager.create.return_value = mockAlgo

    def test_execute_successful():
        # mock algorithm execution result and output
        mockAlgo.execute.return_value = "passed"
        params = PixelGroupingParameters(
            groupID=1, twoTheta=3.14, dResolution=Limit(minimum=0.1, maximum=1.0), dRelativeResolution=0.01
        )
        mock_output_val = [params.json()]
        mockAlgo.getProperty("OutputParameters").value = json.dumps(mock_output_val)

        # execute recipe with mocked input
        recipe = PixelGroupingParametersCalculationRecipe()
        ingredients = mock.Mock(return_value="good ingredients")
        data = recipe.executeRecipe(ingredients)

        assert mockAlgo.execute.called
        assert isinstance(data, dict)
        assert data["result"] is not None
        assert data["result"] == "passed"
        assert isinstance(data["parameters"], list)
        assert data["parameters"][0].json() == mock_output_val[0]

    def test_execute_unsuccessful():
        mockAlgo.execute.side_effect = RuntimeError("passed")

        recipe = PixelGroupingParametersCalculationRecipe()
        ingredients = mock.Mock()

        try:
            recipe.executeRecipe(ingredients)
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

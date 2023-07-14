import unittest
from unittest import mock

import pytest
from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients as TheseIngredients
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as ThisRecipe

PatchThis: str = "snapred.backend.recipe.DiffractionCalibrationRecipe.AlgorithmManager"
CalledAlgo: str = "CalculateOffsetDIFC"
CalledIngredients: str = TheseIngredients.__name__


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        print(CalledIngredients)
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo = mock.MagicMock()
        self.mock_algo.execute.return_value = "Mocked result"

        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_ingredients = mock.MagicMock(spec=TheseIngredients)
        self.mock_ingredients.runConfig = self.mock_runConfig

        self.recipe = ThisRecipe()

    def test_chop_ingredients(self):
        assert not self.recipe.chopIngredients(self.mock_ingredients)

    @mock.patch(PatchThis)
    def test_execute_successful(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.mock_algo

        result = self.recipe.executeRecipe(self.mock_ingredients)

        assert result["result"] == "Mocked result"
        self.mock_algo.execute.assert_called_once()
        self.mock_algo.setProperty.assert_called_once_with(CalledIngredients, self.mock_ingredients.json())
        mock_AlgorithmManager.create.assert_called_once_with(CalledAlgo)

    @mock.patch(PatchThis)
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        self.mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = self.mock_algo

        try:
            self.recipe.executeRecipe(self.mock_ingredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            self.mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.mock_algo.setProperty.assert_called_once_with(CalledIngredients, self.mock_ingredients.json())
        mock_AlgorithmManager.create.assert_called_once_with(CalledAlgo)


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

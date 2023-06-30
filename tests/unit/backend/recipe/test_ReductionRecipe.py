import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe


class TestReductionRecipe(unittest.TestCase):
    def setUp(self):
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo = mock.MagicMock()
        self.mock_algo.execute.return_value = "Mocked result"

        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_reductionIngredients = mock.MagicMock(spec=ReductionIngredients)
        self.mock_reductionIngredients.runConfig = self.mock_runConfig

        # Create an instance of the ReductionRecipe
        self.reductionRecipe = ReductionRecipe()

    @mock.patch("snapred.backend.recipe.ReductionRecipe.AlgorithmManager")
    def test_executeRecipe(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.mock_algo

        # Call the executeRecipe method
        result = self.reductionRecipe.executeRecipe(self.mock_reductionIngredients)

        # Assertions
        assert result["result"] == "Mocked result"
        self.mock_algo.execute.assert_called_once()
        self.mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", self.mock_reductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("ReductionAlgorithm")

    @mock.patch("snapred.backend.recipe.ReductionRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        self.mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = self.mock_algo

        try:
            self.reductionRecipe.executeRecipe(self.mock_reductionIngredients)
        except Exception as ex:  # noqa: E722 BLE001
            assert str(ex) == "passed"  # noqa: PT017
            self.mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", self.mock_reductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("ReductionAlgorithm")


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

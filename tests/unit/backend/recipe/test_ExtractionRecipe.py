import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ExtractionIngredients import ExtractionIngredients
from snapred.backend.recipe.ExtractionRecipe import ExtractionRecipe


class TestExtractionRecipe(unittest.TestCase):
    def setUp(self):
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo = mock.MagicMock()
        self.mock_algo.execute.return_value = "Mocked result"

        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_extractionIngredients = mock.MagicMock(spec=ExtractionIngredients)
        self.mock_extractionIngredients.runConfig = self.mock_runConfig

        self.extractionRecipe = ExtractionRecipe()

    @mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        mock_AlgorithmManager.create.return_value = self.mock_algo

        result = self.extractionRecipe.executeRecipe(self.mock_extractionIngredients)

        assert result["result"] == "Mocked result"
        self.mock_algo.execute.assert_called_once()
        self.mock_algo.setProperty.assert_called_once_with(
            "ExtractionIngredients", self.mock_extractionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("ExtractionAlgorithm")

    @mock.patch("snapred.backend.recipe.ExtractionRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        self.mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = self.mock_algo

        try:
            self.extractionRecipe.executeRecipe(self.mock_extractionIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            self.mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.mock_algo.setProperty.assert_called_once_with(
            "ExtractionIngredients", self.mock_extractionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("ExtractionAlgorithm")


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

import unittest
from unittest import mock

import pytest
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.recipe.FitMultiplePeaksRecipe import FitMultiplePeaksRecipe


class TestFitMultiplePeaksRecipe(unittest.TestCase):
    @mock.patch("snapred.backend.recipe.FitMultiplePeaksRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        # Create a mock algorithm instance and set the expected return value
        mock_algo = mock.MagicMock()
        mock_algo.execute.return_value = "Mocked result"
        mock_AlgorithmManager.create.return_value = mock_algo

        # Create a mock ReductionIngredients instance with the required attributes
        mock_FitMultiplePeaksIngredients = mock.MagicMock(spec=FitMultiplePeaksIngredients)

        fitMultPeaksRecipe = FitMultiplePeaksRecipe()
        result = fitMultPeaksRecipe.executeRecipe(mock_FitMultiplePeaksIngredients)

        assert result["result"] == "Mocked result"
        mock_algo.execute.assert_called_once()
        mock_algo.setProperty.assert_called_once_with(
            "FitMultiplePeaksIngredients", mock_FitMultiplePeaksIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("FitMultiplePeaksAlgorithm")

    @mock.patch("snapred.backend.recipe.FitMultiplePeaksRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        mock_algo = mock.MagicMock()
        mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = mock_algo

        mock_FitMultiplePeaksIngredients = mock.MagicMock(spec=FitMultiplePeaksIngredients)

        fitMultPeaksRecipe = FitMultiplePeaksRecipe()
        try:
            fitMultPeaksRecipe.executeRecipe(mock_FitMultiplePeaksIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        mock_algo.setProperty.assert_called_once_with(
            "FitMultiplePeaksIngredients", mock_FitMultiplePeaksIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("FitMultiplePeaksAlgorithm")

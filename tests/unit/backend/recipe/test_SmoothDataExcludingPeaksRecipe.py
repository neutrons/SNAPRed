import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.recipe.SmoothDataExcludingPeaksRecipe import SmoothDataExcludingPeaksRecipe


class TestSmoothDataExcludingPeaksRecipe(unittest.TestCase):
    @mock.patch("snapred.backend.recipe.SmoothDataExcludingPeaksRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        mock_algo = mock.MagicMock()
        mock_algo.execute.return_value = "Mocked Result"
        mock_AlgorithmManager.create.return_value = mock_algo

        mock_SmoothDataIngredients = mock.MagicMock(spec=PeakIngredients)

        smoothDataRecipe = SmoothDataExcludingPeaksRecipe()
        test_result = smoothDataRecipe.executeRecipe(mock_SmoothDataIngredients)

        assert test_result["result"] == "Mocked Result"
        mock_algo.execute.assert_called_once()
        mock_algo.setProperty.assert_called_once_with("Ingredients", mock_SmoothDataIngredients.json())
        mock_AlgorithmManager.create.assert_called_once_with("SmoothDataExcludingPeaksAlgo")

    @mock.patch("snapred.backend.recipe.SmoothDataExcludingPeaksRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        mock_algo = mock.MagicMock()
        mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = mock_algo

        mock_SmoothDataIngredients = mock.MagicMock(spec=PeakIngredients)

        smoothDataRecipe = SmoothDataExcludingPeaksRecipe()
        try:
            smoothDataRecipe.executeRecipe(mock_SmoothDataIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            mock_algo.execute.assert_called_once()
        else:
            pytest.fail("Test should raise RuntimeError, but no error has been raised.")

        mock_algo.setProperty.assert_called_once_with("Ingredients", mock_SmoothDataIngredients.json())
        mock_AlgorithmManager.create.assert_called_once_with("SmoothDataExcludingPeaksAlgo")

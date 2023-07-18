import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe


class TestVanadiumFocussedReductionRecipe(unittest.TestCase):
    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        # Create a mock algorithm instance and set the expected return value
        mock_algo = mock.MagicMock()
        mock_algo.execute.return_value = "Mocked result"
        mock_AlgorithmManager.create.return_value = mock_algo

        # Create a mock ReductionIngredients instance with the required attributes
        mock_ReductionIngredients = mock.MagicMock(spec=ReductionIngredients)

        vanadiumRecipe = VanadiumFocussedReductionRecipe()
        result = vanadiumRecipe.executeRecipe(mock_ReductionIngredients)

        assert result["result"] == "Mocked result"
        mock_algo.execute.assert_called_once()
        mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", mock_ReductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")

    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        mock_algo = mock.MagicMock()
        mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = mock_algo

        mock_ReductionIngredients = mock.MagicMock(spec=ReductionIngredients)

        vanadiumRecipe = VanadiumFocussedReductionRecipe()
        try:
            vanadiumRecipe.executeRecipe(mock_ReductionIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", mock_ReductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")
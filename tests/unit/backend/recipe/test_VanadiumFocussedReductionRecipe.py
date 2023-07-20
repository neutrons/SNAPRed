import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe


class TestVanadiumFocussedReductionRecipe(unittest.TestCase):
    def setUp(self):
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo = mock.MagicMock()
        self.mock_algo.execute.return_value = "Mocked result"

        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_ReductionIngredients = mock.MagicMock(spec=ReductionIngredients)
        self.mock_ReductionIngredients.runConfig = self.mock_runConfig

        self.vanadiumRecipe = VanadiumFocussedReductionRecipe()

    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo.execute.return_value = "Mocked result"
        mock_AlgorithmManager.create.return_value = self.mock_algo

        result = self.vanadiumRecipe.executeRecipe(self.mock_ReductionIngredients)

        assert result["result"] == "Mocked result"
        self.mock_algo.execute.assert_called_once()
        self.mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", self.mock_ReductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")

    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        self.mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = self.mock_algo

        try:
            self.vanadiumRecipe.executeRecipe(self.mock_ReductionIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            self.mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.mock_algo.setProperty.assert_called_once_with(
            "ReductionIngredients", self.mock_ReductionIngredients.json()
        )
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")

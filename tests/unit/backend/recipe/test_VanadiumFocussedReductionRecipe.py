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

        self.mock_smoothIngredients = mock.MagicMock()
        self.vanadiumRecipe = VanadiumFocussedReductionRecipe()
        self.call_list = [mock.call("ReductionIngredients", mock.ANY), mock.call("SmoothDataIngredients", mock.ANY)]

    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_successful(self, mock_AlgorithmManager):
        # Create a mock algorithm instance and set the expected return value
        self.mock_algo.execute.return_value = "Mocked result"
        mock_AlgorithmManager.create.return_value = self.mock_algo

        result = self.vanadiumRecipe.executeRecipe(self.mock_ReductionIngredients, self.mock_smoothIngredients)

        assert result["result"] == "Mocked result"
        self.mock_algo.execute.assert_called_once()
        assert self.mock_algo.setProperty.call_args_list == self.call_list
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")

    @mock.patch("snapred.backend.recipe.VanadiumFocussedReductionRecipe.AlgorithmManager")
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        self.mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = self.mock_algo

        try:
            self.vanadiumRecipe.executeRecipe(self.mock_ReductionIngredients, self.mock_smoothIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            self.mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        assert self.mock_algo.setProperty.call_args_list == self.call_list
        mock_AlgorithmManager.create.assert_called_once_with("VanadiumFocussedReductionAlgorithm")

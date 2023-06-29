import unittest
from unittest import mock

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe


class TestReductionRecipe(unittest.TestCase):
    @mock.patch("snapred.backend.recipe.ReductionRecipe.AlgorithmManager")
    def test_executeRecipe(self, mock_AlgorithmManager):
        # Create a mock algorithm instance and set the expected return value
        mock_algo = mock.MagicMock()
        mock_algo.execute.return_value = "Mocked result"
        mock_AlgorithmManager.create.return_value = mock_algo

        # Create a mock ReductionIngredients instance with the required attributes
        mock_runConfig = mock.MagicMock()
        mock_runConfig.runNumber = "12345"
        mock_reductionIngredients = mock.MagicMock(spec=ReductionIngredients)
        mock_reductionIngredients.runConfig = mock_runConfig

        # Create an instance of the ReductionRecipe
        reductionRecipe = ReductionRecipe()

        # Call the executeRecipe method
        result = reductionRecipe.executeRecipe(mock_reductionIngredients)

        # Assertions
        assert result["result"] == "Mocked result"
        mock_algo.execute.assert_called_once()
        mock_algo.setProperty.assert_called_once_with("ReductionIngredients", mock_reductionIngredients.json())
        mock_AlgorithmManager.create.assert_called_once_with("ReductionAlgorithm")

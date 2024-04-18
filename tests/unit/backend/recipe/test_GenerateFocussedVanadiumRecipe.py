import unittest
from unittest import mock

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import GenerateFocussedVanadiumIngredients as Ingredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe


class TestGenerateFocussedVanadiumRecipe(unittest.TestCase):
    def setUp(self):
        detectorPeaksList = mock.MagicMock(spec=GroupPeakList)
        self.ingredients = Ingredients(smoothingParameter=0.1, detectorPeaks=[detectorPeaksList])
        self.groceries = {"intputWorkspace": "inputWS", "outputWorkspace": "outputWS"}
        self.recipe = GenerateFocussedVanadiumRecipe()

        self.mock_algo = mock.create_autospec(SmoothDataExcludingPeaksAlgo)
        self.mock_algo.getPropertyValue.return_value = "Mocked output workspace"

        self.patcher = mock.patch(
            "snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo.SmoothDataExcludingPeaksAlgo",
            return_value=self.mock_algo,
        )
        self.addCleanup(self.patcher.stop)
        self.patcher.start()

    def test_execute_recipe_success(self):
        result = self.recipe.executeRecipe(self.ingredients, self.groceries)
        self.assertTrue(result["result"])  # noqa: PT009
        self.assertEqual(result["outputWorkspace"], "Mocked output workspace")  # noqa: PT009

    def test_execute_recipe_failure(self):
        self.mock_algo.execute.side_effect = RuntimeError("Execution failed")
        with self.assertRaises(RuntimeError) as context:  # noqa: PT027
            self.recipe.executeRecipe(self.ingredients, self.groceries)  # noqa: PT009
        self.assertIn("Execution failed", str(context.exception))  # noqa: PT009

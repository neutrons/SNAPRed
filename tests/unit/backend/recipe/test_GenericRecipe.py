import unittest
from unittest import mock

import pytest
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.GenericRecipe import GenericRecipe


class DummyAlgo:
    pass


class TestGenericRecipe(unittest.TestCase):
    def setUp(self):
        # Create a mock ReductionIngredients instance with the required attributes
        self.mock_runConfig = mock.MagicMock()
        self.mock_runConfig.runNumber = "12345"
        self.mock_reductionIngredients = mock.MagicMock(spec=ReductionIngredients)
        self.mock_reductionIngredients.runConfig = self.mock_runConfig

        class CakeRecipe(GenericRecipe[DummyAlgo]):
            pass

        self.GenericRecipe = CakeRecipe()
        self.GenericRecipe.mantidSnapper = mock.Mock()
        self.mockReturn = mock.Mock()
        self.mockReturn.get.return_value = "Mocked result"
        # self.GenericRecipe.mantidSnapper.DummyAlgo = lambda x:"Mocked result"
        self.GenericRecipe.mantidSnapper.DummyAlgo.return_value = self.mockReturn

    @mock.patch("snapred.backend.recipe.GenericRecipe.MantidSnapper")
    def test_execute_successful(self, mock_MantidSnapper):  # noqa: ARG002
        result = self.GenericRecipe.executeRecipe(Input=self.mock_reductionIngredients)

        assert result == "Mocked result"
        self.GenericRecipe.mantidSnapper.DummyAlgo.assert_called_once_with(
            "", Input=self.mock_reductionIngredients.json()
        )

    @mock.patch("snapred.backend.recipe.GenericRecipe.MantidSnapper")
    def test_execute_unsuccessful(self, mock_MantidSnapper):  # noqa: ARG002
        self.GenericRecipe.mantidSnapper.executeQueue.side_effect = RuntimeError("passed")

        try:
            self.GenericRecipe.executeRecipe(Input=self.mock_reductionIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        self.GenericRecipe.mantidSnapper.DummyAlgo.assert_called_once_with(
            "", Input=self.mock_reductionIngredients.json()
        )

import unittest
from unittest.mock import Mock, patch

import pytest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.service.ReductionService import ReductionService


class TestReductionService(unittest.TestCase):
    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduction_calls_executeRecipe_with_correct_arguments(
        self,
        mock_executeRecipe,
    ):
        run = RunConfig(
            runNumber="123",
            useLiteMode=True,
        )
        self.instance = ReductionService()
        self.instance.sousChef.prepReductionIngredients = Mock()
        mock_executeRecipe.return_value = {"not": "empty"}

        res = self.instance.reduce([run])

        assert self.instance.sousChef.prepReductionIngredients.called
        assert mock_executeRecipe.called_with(
            Ingredients=self.instance.sousChef.prepReductionIngredients.return_value,
        )
        assert res == mock_executeRecipe.return_value

    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_fails(self, mock_executeRecipe):
        run = RunConfig(
            runNumber="123",
            useLiteMode=True,
        )
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        self.instance = ReductionService()
        self.instance.sousChef.prepReductionIngredients = Mock()

        with pytest.raises(RuntimeError) as e:
            self.instance.reduce([run])
        assert "oops!" in str(e.value)

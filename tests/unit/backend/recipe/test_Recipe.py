import unittest
from typing import Dict, Set

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from pydantic import BaseModel, ConfigDict
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from util.SculleryBoy import SculleryBoy


class Ingredients(BaseModel):
    name: str
    model_config = ConfigDict(extra="forbid")


class DummyRecipe(Recipe[Ingredients]):
    """
    An instantiation with no abstract classes
    """

    def mandatoryInputWorkspaces(self) -> Set[WorkspaceName]:
        return {"ws"}

    def chopIngredients(self, ingredients: Ingredients):
        pass

    def unbagGroceries(self, groceries: Dict[str, str]):
        pass

    def queueAlgos(self):
        pass


class RecipeTest(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def _make_groceries(self):
        wsname = mtd.unique_name(prefix="test_recipe")
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        return {"ws": wsname}

    def test_init(self):
        DummyRecipe()

    def test_init_reuseUtensils(self):
        utensils = Utensils()
        utensils.PyInit()
        recipe = DummyRecipe(utensils=utensils)
        assert recipe.mantidSnapper == utensils.mantidSnapper

    def test_validate_inputs(self):
        groceries = self._make_groceries()
        ingredients = Ingredients(name="validating")
        recipe = DummyRecipe()
        recipe.validateInputs(ingredients, groceries)

        badIngredients = {"nothing": 0}
        with pytest.raises(ValueError, match="Ingredients must be a Pydantic BaseModel."):
            recipe.validateInputs(badIngredients, groceries)
        badGroceries = {"ws": mtd.unique_name(prefix="bad")}
        with pytest.raises(RuntimeError):
            recipe.validateInputs(ingredients, badGroceries)

    def test_cook(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = DummyRecipe(utensils=untensils)
        ingredients = Ingredients(name="cooking")
        groceries = self._make_groceries()

        assert recipe.cook(ingredients, groceries)

        assert mockSnapper.executeQueue.called

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = unittest.mock.Mock()
        untensils.mantidSnapper = mockSnapper
        recipe = DummyRecipe(utensils=untensils)
        ingredients = Ingredients(name="catering")
        groceries = self._make_groceries()

        assert recipe.cater([(ingredients, groceries), (ingredients, groceries)])

        assert mockSnapper.executeQueue.call_count == 1

    ## The below tests verify the abstract methods must be initialized

    def test_chopIngredients(self):
        class RecipeNoChopIngredients(Recipe[Ingredients]):
            def unbagGroceries(self, groceries: Dict[str, str]):
                pass

            def queueAlgos(self):
                pass

        with pytest.raises(TypeError) as e:
            RecipeNoChopIngredients()
        assert "chopIngredients" in str(e)
        assert "unbagGroceries" not in str(e)
        assert "queueAlgos" not in str(e)

    def test_unbagGroceries(self):
        class RecipeNoUnbagGroceries(Recipe[Ingredients]):
            def chopIngredients(self, ingredients: Ingredients):
                pass

            def queueAlgos(self):
                pass

        with pytest.raises(TypeError) as e:
            RecipeNoUnbagGroceries()
        assert "chopIngredients" not in str(e)
        assert "unbagGroceries" in str(e)
        assert "queueAlgos" not in str(e)

    def test_queueAlgos(self):
        class RecipeNoQueueAlgos(Recipe[Ingredients]):
            def chopIngredients(self, ingredients: Ingredients):
                pass

            def unbagGroceries(self, groceries: Dict[str, str]):
                pass

        with pytest.raises(TypeError) as e:
            RecipeNoQueueAlgos()
        assert "chopIngredients" not in str(e)
        assert "unbagGroceries" not in str(e)
        assert "queueAlgos" in str(e)

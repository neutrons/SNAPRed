import unittest
from typing import Dict, Set

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from pydantic import BaseModel, ConfigDict
from util.SculleryBoy import SculleryBoy

from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe


class Ingredients(BaseModel):
    name: str
    model_config = ConfigDict(extra="forbid")


class FakeIngredients(BaseModel):
    name: str
    name2: str


class DummyRecipe(Recipe[Ingredients]):
    """
    An instantiation with no abstract classes
    """

    def allGroceryKeys(self) -> Set[str]:
        return {"ws"}

    def mandatoryInputWorkspaces(self) -> Set[str]:
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

    def test_Ingredients(self):
        assert Ingredients(name="test") == DummyRecipe.Ingredients(name="test")

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
        with pytest.raises(
            RuntimeError, match=f"The indicated workspace {badGroceries['ws']} not found in Mantid ADS."
        ):
            recipe.validateInputs(ingredients, badGroceries)
        worseGroceries = {}
        with pytest.raises(RuntimeError, match="The workspace property ws was not found in the groceries"):
            recipe.validateInputs(ingredients, worseGroceries)
        tooManyGroceries = {"ws": groceries["ws"], "another": groceries["ws"]}
        with pytest.raises(ValueError, match=r".*invalid keys.*"):
            recipe.validateInputs(ingredients, tooManyGroceries)

    def test_validate_grocery(self):
        groceries = self._make_groceries()
        recipe = DummyRecipe()

        with pytest.raises(ValueError, match="The key for the grocery must be a string."):
            recipe._validateGrocery(123, groceries["ws"])

    def test_validate_ingredients_fail(self):
        fakeIngredients = FakeIngredients(name="fake", name2="fake2")
        recipe = DummyRecipe()

        with pytest.raises(ValueError, match=r".*Invalid ingredients*"):
            recipe._validateIngredients(fakeIngredients)

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

    def test_allGroceryKeys(self):
        class RecipeNoGroceryKeys(Recipe[Ingredients]):
            def unbagGroceries(self, groceries: Dict[str, str]):
                pass

            def queueAlgos(self):
                pass

        with pytest.raises(TypeError) as e:
            RecipeNoGroceryKeys()
        assert "allGroceryKeys" in str(e)
        assert "unbagGroceries" not in str(e)
        assert "queueAlgos" not in str(e)

        class RecipeGroceryKeys(Recipe[Ingredients]):
            def allGroceryKeys(self):
                return super().allGroceryKeys()

            def chopIngredients(self, ingredients):
                pass

            def unbagGroceries(self, groceries):
                pass

            def queueAlgos(self):
                pass

        assert set() == RecipeGroceryKeys().allGroceryKeys()

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

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

from mantid.api import AlgorithmManager
from pydantic import BaseModel as Ingredients

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


class Recipe(ABC):
    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    @abstractmethod
    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        raise NotImplementedError("Recipes must implement the chopIngredients method")

    @abstractmethod
    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        """
        Unpacks the workspace data from the groceries.
        """
        raise NotImplementedError("Recipes must implement the unbagGroceries method")

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        """
        Validate the input properties before chopping or unbagging
        """
        Ingredients.parse_obj(ingredients)
        for ws in groceries.values():
            if not self.mantidSnapper.mtd.doesExist(ws):
                raise RuntimeError(f"The indicated workspace {ws} not found in Mantid ADS.")

    def stirInputs(self):
        """
        Validation of chopped and unbagged inputs, to ensure consistency
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

    @abstractmethod
    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """
        raise NotImplementedError("Recipes must implement the queueAlgos method")

    def prep(self, ingredients: Any, groceries: Dict[str, str]):
        """
        Convinience method to prepare the recipe for execution.
        """
        self.validateInputs(ingredients, groceries)
        self.unbagGroceries(groceries)
        self.chopIngredients(ingredients)
        self.stirInputs()

        self.queueAlgos()

    def execute(self):
        """
        Final step in a recipe, executes the queued algorithms.
        Requires: queued algorithms.
        """
        self.mantidSnapper.executeQueue()
        return True

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> bool:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        return self.execute()

    def cater(self, shipment: List[Pallet]) -> bool:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        for ingredient, grocery in shipment:
            self.prep(ingredient, grocery)
        return self.execute()

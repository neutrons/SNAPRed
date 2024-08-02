from abc import ABC, abstractmethod
from typing import Dict, Generic, List, Tuple, TypeVar, get_args

from pydantic import BaseModel, ValidationError

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Ingredients = TypeVar("Ingredients", bound=BaseModel)

Pallet = Tuple[Ingredients, Dict[str, str]]


class Recipe(ABC, Generic[Ingredients]):
    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    ## Abstract methods which MUST be implemented

    @abstractmethod
    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """

    @abstractmethod
    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        """
        Unpacks the workspace data from the groceries.
        """

    @abstractmethod
    def queueAlgos(self):
        """
        Queues up the procesing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """

    # methods which MAY be kept as is

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        """
        Validate the input properties before chopping or unbagging
        """
        if ingredients is not None and not isinstance(ingredients, BaseModel):
            raise ValueError("Ingredients must be a Pydantic BaseModel.")
        # cast the ingredients into the Ingredients type
        try:
            # to run the same as Ingredients.model_validate(ingredients)
            if ingredients is not None:
                get_args(self.__orig_bases__[0])[0].model_validate(ingredients.dict())
        except ValidationError as e:
            raise e
        # ensure all of the given workspaces exist
        # NOTE may need to be tweaked to ignore output workspaces...
        logger.info(f"Validating the given workspaces: {groceries.values()}")
        for ws in groceries.values():
            if not isinstance(ws, list):
                ws = [ws]
            for wsStr in ws:
                if isinstance(wsStr, str) and not self.mantidSnapper.mtd.doesExist(wsStr):
                    raise RuntimeError(f"The indicated workspace {wsStr} not found in Mantid ADS.")

    def stirInputs(self):
        """
        Validation of chopped and unbagged inputs, to ensure consistency
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

    def prep(self, ingredients: Ingredients, groceries: Dict[str, str]):
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

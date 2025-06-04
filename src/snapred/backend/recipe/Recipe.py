from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Set, Tuple, TypeVar, get_args

from pydantic import BaseModel, ValidationError

from snapred.backend.api.HookManager import HookManager
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)

Ingredients = TypeVar("Ingredients", bound=BaseModel)

Pallet = Tuple[Ingredients, Dict[str, str]]


class Recipe(ABC, Generic[Ingredients]):
    _Ingredients: Any

    def __init__(self, utensils: Utensils = None):
        """
        Sets up the recipe with the necessary utensils.
        """
        # NOTE: workaround, we just add an empty host algorithm.
        self.utensils = utensils
        if self.utensils is None:
            self.utensils = Utensils()
            self.utensils.PyInit()
        self.mantidSnapper = self.utensils.mantidSnapper
        self.hookManager = HookManager()

    def __init_subclass__(cls) -> None:
        cls._Ingredients = get_args(cls.__orig_bases__[0])[0]  # type: ignore

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
        Queues up the processing algorithms for the recipe.
        Requires: unbagged groceries and chopped ingredients.
        """

    @abstractmethod
    def allGroceryKeys(self) -> Set[str]:
        """
        A set of all possible keys which may be in the grocery dictionary
        """
        return set()

    # methods which MAY be kept as is

    def mandatoryInputWorkspaces(self) -> Set[str]:
        """
        A set of workspace keys corresponding to mandatory inputs
        """
        return set()

    @classmethod
    def Ingredients(cls, **kwargs):
        return cls._Ingredients(**kwargs)

    def _validateGrocery(self, key, ws):
        """
        Validate the given grocery-list entry
        """
        if ws is None:
            raise RuntimeError(f"The required workspace property '{key}' was not found in the grocery list")

        if not isinstance(key, str):
            raise ValueError(f"The keys for the grocery list must be strings, not '{key}'.")

        if not isinstance(ws, list):
            ws = [ws]

        for wsStr in ws:
            if isinstance(wsStr, str) and not self.mantidSnapper.mtd.doesExist(wsStr):
                raise RuntimeError(
                    f"The required workspace property for key '{key}': '{wsStr}' was not found in the Mantid ADS."
                )

    def _validateIngredients(self, ingredients: Ingredients):
        """
        Validate the ingredients before chopping
        """
        if ingredients is not None and not isinstance(ingredients, BaseModel):
            raise ValueError("Ingredients must be a Pydantic BaseModel.")
        # cast the ingredients into the Ingredients type
        try:
            # to run the same as Ingredients.model_validate(ingredients)
            if ingredients is not None:
                self._Ingredients.model_validate(ingredients.dict())
        except ValidationError as e:
            raise ValueError(f"Invalid ingredients: {e}") from e

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        """
        Validate the input properties before chopping or unbagging
        """
        if ingredients is not None:
            self._validateIngredients(ingredients)
        else:
            logger.debug("No ingredients were given, skipping ingredient validation")

        # make sure no invalid keys were passed
        if groceries is not None:
            diff = set(groceries.keys()).difference(self.allGroceryKeys())
            if bool(diff):
                raise ValueError(f"The following invalid keys were found in the input groceries: {diff}")

        # ensure all of the given workspaces exist
        # NOTE may need to be tweaked to ignore output workspaces...
        if groceries is not None:
            logger.debug(f"Validating the given workspaces: {groceries.values()}")
            for key in self.mandatoryInputWorkspaces():
                ws = groceries.get(key)
                self._validateGrocery(key, ws)
        else:
            logger.debug("No groceries were given, skipping workspace validation")

    def stirInputs(self):
        """
        Validation of chopped and unbagged inputs, to ensure consistency
        Requires: unbagged groceries and chopped ingredients.
        """
        pass

    def prep(self, ingredients: Ingredients, groceries: Dict[str, str]):
        """
        Convenience method to prepare the recipe for execution.
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
        recipeName = self.__class__.__name__
        self.hookManager.execute(f"Pre{recipeName}", self)
        self.prep(ingredients, groceries)
        result = self.execute()
        self.hookManager.execute(f"Post{recipeName}", self)
        return result

    def cater(self, shipment: List[Pallet]) -> bool:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        for ingredient, grocery in shipment:
            self.prep(ingredient, grocery)
        return self.execute()

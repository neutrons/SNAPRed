import json
from typing import Any, Dict

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    FitMultiplePeaksRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


class FitCalibrationWorkspaceRecipe:
    """
    Fits Peaks of a given workspace using data that can be found on disk.
    Plus some calculated Pixel Grouping Parameters.
    """

    def __init__(self):
        pass

    # would be nice to have a from kwarg decorator...
    def executeRecipe(self, ingredients: Ingredients, grocery: WorkspaceName) -> Dict[str, Any]:
        logger.info(f"Executing recipe for: {ingredients.workspaceName}")

        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=grocery,
            Ingredients=ingredients,
            OutputWorkspace=grocery,
        )

        fitResult = FitMultiplePeaksRecipe().executeRecipe(
            InputWorkspace=grocery,
            Ingredients=ingredients,
            OutputWorkspaceGroup=f"fitted_{grocery}",
        )
        logger.info(f"Finished smoothing and fitting workspace: {self.workspaceName}")
        return fitResult

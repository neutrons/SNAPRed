import json
from typing import Any, Dict

from snapred.backend.dao.ingredients import (
    FitCalibrationWorkspaceIngredients,
    FitMultiplePeaksIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.GenericRecipe import (
    FitMultiplePeaksRecipe,
    SmoothDataExcludingPeaksRecipe,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


class FitCalibrationWorkspaceRecipe:
    """
    Fits Peaks of a given workspace using data that can be found on disk.
    Plus some calculated Pixel Grouping Parameters.
    """

    def __init__(self):
        pass

    def chop(self, ingredients: FitCalibrationWorkspaceIngredients):
        """
        Extract and prep data from valided object.
        """
        self.instrumentState = ingredients.instrumentState
        self.crystalInfo = ingredients.crystalInfo
        self.workspaceName = ingredients.workspaceName
        self.pixelGroupingParameters = ingredients.pixelGroupingParameters
        self.smoothingParameter = ingredients.smoothingParameter

    def portion(self):
        """
        Prep most ingredients for sub-recipes.
        """
        self.smoothingIngredients = SmoothDataExcludingPeaksIngredients(
            instrumentState=self.instrumentState,
            crystalInfo=self.crystalInfo,
            smoothingParameter=self.smoothingParameter,
        )
        self.fitIngredients = FitMultiplePeaksIngredients(
            instrumentState=self.instrumentState,
            crystalInfo=self.crystalInfo,
            inputWorkspace=f"{self.workspaceName}(smooth+stripped)",
        )

    # would be nice to have a from kwarg decorator...
    def executeRecipe(self, ingredients: FitCalibrationWorkspaceIngredients) -> Dict[str, Any]:
        logger.info(f"Executing recipe for: {ingredients.workspaceName}")
        self.chop(ingredients)
        self.portion()

        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=self.workspaceName,
            SmoothDataExcludingPeaksIngredients=self.smoothingIngredients,
            OutputWorkspace=self.fitIngredients.InputWorkspace,
        )

        fitResult = FitMultiplePeaksRecipe().executeRecipe(
            FitMultiplePeaksIngredients=self.fitIngredients,
            OutputWorkspaceGroup=f"fitted_{self.fitIngredients.InputWorkspace}",
        )
        logger.info(f"Finished smoothing and fitting workspace: {self.workspaceName}")
        return fitResult

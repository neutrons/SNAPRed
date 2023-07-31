import json
from typing import Any, Dict

from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.dao.ingredients.FitCalibrationWorkspaceIngredients import FitCalibrationWorkspaceIngredients
from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
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

    def portion(self):
        """
        Prep most ingredients for sub-recipes.
        """
        self.smoothIngredients = SmoothDataExcludingPeaksIngredients(
            instrumentState=self.instrumentState, crystalInfo=self.crystalInfo
        )
        self.fitIngredients = FitMultiplePeaksIngredients(
            InstrumentState=self.instrumentState,
            CrystalInfo=self.crystalInfo,
            InputWorkspace=self.workspaceName + "(smooth+stripped)",
        )

    # would be nice to have a from kwarg decorator...
    def executeRecipe(self, ingredients: FitCalibrationWorkspaceIngredients) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.workspaceName)
        self.chop(ingredients)
        self.portion()

        SmoothDataExcludingPeaksRecipe().executeRecipe(
            InputWorkspace=self.workspaceName,
            SmoothDataExcludingPeaksIngredients=self.smoothIngredients,
            OutputWorkspace=self.fitIngredients.InputWorkspace,
        )

        fitResult = FitMultiplePeaksRecipe().executeRecipe(
            FitMultiplePeaksIngredients=self.fitIngredients,
            OutputWorkspaceGroup="fitted_{}".format(self.fitIngredients.InputWorkspace),
        )
        logger.info("Finished smoothing and fitting workspace: %s" % self.workspaceName)
        return fitResult

import json
from typing import Any, Dict, List

from pydantic import parse_raw_as

from snapred.backend.dao.ingredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import BinnedValue
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
    PixelGroupingParametersCalculationAlgorithm,
)
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


@Singleton
class PixelGroupingParametersCalculationRecipe:
    def __init__(self):
        # NOTE: workaround, we just add an empty host algorithm.
        utensils = Utensils()
        utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def executeRecipe(
        self, ingredients: PixelGroupingIngredients, groceries: Dict[str, WorkspaceName]
    ) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.groupingScheme)
        data: Dict[str, Any] = {}

        res = self.mantidSnapper.PixelGroupingParametersCalculationAlgorithm(
            "Calling algorithm",
            Ingredients=ingredients.json(),
            GroupingWorkspace=groceries["groupingWorkspace"],
            MaskWorkspace=groceries.get("MaskWorkspace", ""),
        )
        self.mantidSnapper.executeQueue()
        # NOTE contradictory issues with Callbacks between GUI and unit tests
        if hasattr(res, "get"):
            res = res.get()

        data["result"] = True
        pixelGroupingParams = parse_raw_as(List[PixelGroupingParameters], res)
        data["parameters"] = pixelGroupingParams
        data["tof"] = BinnedValue(
            minimum=ingredients.instrumentState.particleBounds.tof.minimum,
            maximum=ingredients.instrumentState.particleBounds.tof.maximum,
            binWidth=min(
                [abs(pgp.dRelativeResolution) / ingredients.nBinsAcrossPeakWidth for pgp in pixelGroupingParams]
            ),
            binningMode=BinnedValue.BinningMode.LOG,
        )

        logger.info("Finished pixel grouping parameters calculation.")
        return data

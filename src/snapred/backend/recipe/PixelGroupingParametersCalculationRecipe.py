from typing import Any, Dict, List

import pydantic

from snapred.backend.dao.ingredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import BinnedValue
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


class PixelGroupingParametersCalculationRecipe:
    def __init__(self):
        # NOTE: workaround, we just add an empty host algorithm.
        utensils = Utensils()
        utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    @staticmethod
    def parsePGPList(src: str) -> List[PixelGroupingParameters]:
        # Split out as a separate method, in order to facilitate testing
        return pydantic.TypeAdapter(List[PixelGroupingParameters]).validate_json(src)

    def executeRecipe(
        self, ingredients: PixelGroupingIngredients, groceries: Dict[str, WorkspaceName]
    ) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.groupingScheme)
        data: Dict[str, Any] = {}

        res = self.mantidSnapper.PixelGroupingParametersCalculationAlgorithm(
            "Calling algorithm",
            Ingredients=ingredients.json(),
            GroupingWorkspace=groceries["groupingWorkspace"],
            MaskWorkspace=groceries.get("maskWorkspace", ""),
        )
        self.mantidSnapper.executeQueue()
        # NOTE contradictory issues with Callbacks between GUI and unit tests
        if hasattr(res, "get"):
            res = res.get()

        data["result"] = True
        pixelGroupingParams = self.parsePGPList(res)
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

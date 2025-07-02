from typing import Any, Dict, List

import numpy as np
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
        groupingWorkspace = groceries["groupingWorkspace"]
        maskWorkspace = groceries.get("maskWorkspace", "")
        data: Dict[str, Any] = {}

        result = self.mantidSnapper.PixelGroupingParametersCalculation(
            f"Calculating {'masked ' if bool(maskWorkspace) else ''}pixel-grouping parameters "
            + f"for '{ingredients.groupingScheme if bool(ingredients.groupingScheme) else groupingWorkspace}' grouping",
            Ingredients=ingredients.json(),
            GroupingWorkspace=groupingWorkspace,
            MaskWorkspace=maskWorkspace,
        )
        self.mantidSnapper.executeQueue()

        # NOTE contradictory issues with Callbacks between GUI and unit tests
        if hasattr(result, "get"):
            result = result.get()

        data["result"] = True
        pgps = self.parsePGPList(result)
        data["parameters"] = pgps
        data["tof"] = BinnedValue(
            minimum=ingredients.instrumentState.particleBounds.tof.minimum,
            maximum=ingredients.instrumentState.particleBounds.tof.maximum,
            # WARNING: if all subgroups are fully masked, the PGP-list will be empty!
            binWidth=min([abs(pgp.dRelativeResolution) / ingredients.nBinsAcrossPeakWidth for pgp in pgps])
            if len(pgps) > 0
            else np.nan,
            binningMode=BinnedValue.BinningMode.LOG,
        )

        logger.info("Finished pixel grouping parameters calculation.")
        return data

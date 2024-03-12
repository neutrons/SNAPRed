import json
from typing import Any, Dict, List

from mantid.api import AlgorithmManager
from pydantic import parse_raw_as

from snapred.backend.dao.ingredients import PixelGroupingIngredients
from snapred.backend.dao.Limit import BinnedValue
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
    PixelGroupingParametersCalculationAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


@Singleton
class PixelGroupingParametersCalculationRecipe:
    PixelGroupingParametersCalculationAlgorithmName: str = PixelGroupingParametersCalculationAlgorithm.__name__

    def __init__(self):
        pass

    def executeRecipe(
        self, ingredients: PixelGroupingIngredients, groceries: Dict[str, WorkspaceName]
    ) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.groupingScheme)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.PixelGroupingParametersCalculationAlgorithmName)

        algo.setProperty("Ingredients", ingredients.json())
        algo.setProperty("GroupingWorkspace", groceries["groupingWorkspace"])
        
        # MaskWorkspace: optional argument
        algo.setProperty("MaskWorkspace", groceries.get("maskWorkspace", ""))

        try:
            data["result"] = algo.execute()
            # parse the algorithm output and create a list of calculated PixelGroupingParameters
            pixelGroupingParams = parse_raw_as(
                List[PixelGroupingParameters],
                algo.getProperty("OutputParameters").value,
            )
            data["parameters"] = pixelGroupingParams
            data["tof"] = BinnedValue(
                minimum=ingredients.instrumentState.particleBounds.tof.minimum,
                maximum=ingredients.instrumentState.particleBounds.tof.maximum,
                binWidth=min(
                    [abs(pgp.dRelativeResolution) / ingredients.nBinsAcrossPeakWidth for pgp in pixelGroupingParams]
                ),
                binningMode=BinnedValue.BinningMode.LOG,
            )
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished pixel grouping parameters calculation.")
        return data

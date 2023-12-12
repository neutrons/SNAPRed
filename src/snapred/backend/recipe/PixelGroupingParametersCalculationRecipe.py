import json
from typing import Any, Dict, List

from mantid.api import AlgorithmManager
from pydantic import parse_raw_as

from snapred.backend.dao.ingredients import PixelGroupingIngredients
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import PixelGroupingParametersCalculationAlgorithm
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class PixelGroupingParametersCalculationRecipe:
    PixelGroupingParametersCalculationAlgorithmName: str = PixelGroupingParametersCalculationAlgorithm.__name__

    def __init__(self):
        pass

    def executeRecipe(self, ingredients: PixelGroupingIngredients, groupingWorkspace: str) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.groupingScheme)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.PixelGroupingParametersCalculationAlgorithmName)

        algo.setProperty("InstrumentState", ingredients.instrumentState.json())
        algo.setProperty("GroupingWorkspace", groupingWorkspace)

        try:
            data["result"] = algo.execute()
            # parse the algorithm output and create a list of calculated PixelGroupingParameters
            pixelGroupingParams = parse_raw_as(
                List[PixelGroupingParameters],
                algo.getProperty("OutputParameters").value,
            )
            data["parameters"] = pixelGroupingParams
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished pixel grouping parameters calculation.")
        return data

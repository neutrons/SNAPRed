import json
from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
    PixelGroupingParametersCalculationAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class PixelGroupingParametersCalculationRecipe:
    PixelGroupingParametersCalculationAlgorithmName: str = PixelGroupingParametersCalculationAlgorithm.__name__

    def __init__(self):
        pass

    def executeRecipe(self, ingredients: PixelGroupingIngredients) -> Dict[str, Any]:
        logger.info("Executing recipe for: %s" % ingredients.groupingFile)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.PixelGroupingParametersCalculationAlgorithmName)

        algo.setProperty("InstrumentState", ingredients.instrumentState.json())
        algo.setProperty("InstrumentDefinitionFile", ingredients.instrumentDefinitionFile)
        algo.setProperty("GroupingFile", ingredients.groupingFile)

        try:
            data["result"] = algo.execute()
            # parse the algorithm output and create a list of calculated PixelGroupingParameters
            pixelGroupingParams_json = json.loads(algo.getProperty("OutputParameters").value)
            pixelGroupingParams = []
            for item in pixelGroupingParams_json:
                pixelGroupingParams.append(PixelGroupingParameters.parse_raw(item))
            data["parameters"] = pixelGroupingParams
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished pixel grouping parameters calculation.")
        return data

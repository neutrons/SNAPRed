from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CrystallographicInfoRecipe:
    ingestionAlgorithmName: str = IngestCrystallographicInfoAlgorithm.__name__

    def __init__(self):
        pass

    def executeRecipe(self, cifPath: str, dMin: float = 0.1, dMax: float = 100.0) -> Dict[str, Any]:
        logger.info("Ingesting crystal info: %s" % cifPath)
        data: Dict[str, Any] = {}
        algo = AlgorithmManager.create("IngestCrystallographicInfoAlgorithm")
        algo.setProperty("cifPath", cifPath)
        algo.setProperty("dMin", dMin)
        algo.setProperty("dMax", dMax)

        try:
            data["result"] = algo.execute()
            data["crystalInfo"] = CrystallographicInfo.parse_raw(algo.getProperty("crystalInfo").value)
            data["fSquaredThreshold"] = self.findFSquaredThreshold(data["crystalInfo"])
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished ingesting crystal info: %s" % cifPath)
        return data

    def findFSquaredThreshold(self, xtal: CrystallographicInfo):
        # set a threshold for weak peaks in the spectrum
        # this uses the median of lowest 1% of intensities

        # intensity is fsq times multiplicities
        I0 = [ff * mm * (dd**4) for ff, mm, dd in zip(xtal.fSquared, xtal.multiplicities, xtal.dSpacing)]
        I0.sort()

        # take lowest one percent
        numPeaks = len(xtal.fSquared)
        lowest = max(1, round(numPeaks / 100))

        # get the subset of I0 corresponding to the lowest 1%
        subset = I0[:lowest]

        # compute the median of this subset
        if len(subset) % 2 == 0:  # even length
            median = (subset[len(subset)//2 - 1] + subset[len(subset)//2]) / 2.0
        else:  # odd length
            median = subset[len(subset)//2]
        
        return median

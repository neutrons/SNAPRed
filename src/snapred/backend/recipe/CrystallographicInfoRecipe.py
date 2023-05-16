from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
    name as IngestCrystallographicInfoAlgorithm,
)
from snapred.meta.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CrystallographicInfoRecipe:
    ingestionAlgorithmName: str = IngestCrystallographicInfoAlgorithm

    def __init__(self):
        pass

    def executeRecipe(self, cifPath: str) -> Dict[str, Any]:
        logger.info("Ingesting crystal info: %s" % cifPath)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.ingestionAlgorithmName)
        algo.setProperty("cifPath", cifPath)

        try:
            data["result"] = algo.execute()
            data["crystalInfo"] = CrystallographicInfo.parse_raw(algo.getProperty("crystalInfo").value)
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        data["fSquaredThreshold"] = self.findFSquaredThreshold(data["crystalInfo"])
        logger.info("Finished ingesting crystal info: %s" % cifPath)
        return data

    def findFSquaredThreshold(self, xtal: CrystallographicInfo):
        # set a threshold for weak peaks in the spectrum
        # this uses the median of lowest 1% of intensities

        # intensity is fsq times multiplicities
        I0 = [ff * mm for ff, mm in zip(xtal.fSquared, xtal.multiplicities)]
        I0.sort()

        # take lowest one percent
        numPeaks = len(xtal.fSquared)
        lowest = max(1, round(numPeaks / 100)) - 1
        return I0[int(lowest / 2)]

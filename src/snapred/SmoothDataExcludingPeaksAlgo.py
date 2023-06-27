from typing import Any, Dict

from mantid.simpleapi import *
from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
    name as IngestCrystallographicInfoAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class SmoothDataExcludingPeaks:
    ingestionAlgorithmName: str = IngestCrystallographicInfoAlgorithm

    def __init__(self):
        pass
    
    def executeAlgo(self):
        algo = AlgorithmManager.create(self.ingestionAlgorithmName)

        xtalRx = CrystallographicInfoRecipe()
        xtalData = xtalRx.executeRecipe("/home/dzj/Documents/Work/Silicon_NIST_640d.cif")
        xtalInfo = xtalData["crystalInfo"]

        return xtalInfo
        #CrystallographicPeaks = xtalData["CrystallographicPeak"]

    # NEEDS TO BE FIXED FOR PROPER INPUTS
    # def PrintPeaks(self, xtal: CrystallographicInfo):
    #     peak = xtal

    #     peak_values = [(peak.peaks, peak.fSquared * peak.multiplicities) for peak.peaks in peak]

    #     peak_values.sort(key=lambda x: x[1], reverse=True)

    #     for peak.peaks, value in peak_values:
    #         print(peak, value)

if __name__ == "__main__":
    data = SmoothDataExcludingPeaks()
    xtal = data.executeAlgo()
    data.PrintPeaks(xtal)
from typing import Any, Dict

from mantid.simpleapi import *
from mantid.api import AlgorithmManager

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import PixelGroupingParameters
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
    def PrintPeaks(self, xtal: CrystallographicInfo, pixel_params: PixelGroupingParameters, factor):

        DelOverD = pixel_params.dRelativeResolution
        peaks = xtal.peaks
        
        for xtal in peaks:
            standard_deviation = DelOverD * xtal.dSpacing
            FWHM = standard_deviation * 2.35
            adjusted_FWHM = FWHM * factor
            peak_value = xtal.fSquared * xtal.multiplicity

        print("\n\n")
        print("Peak:", xtal)
        print("Value:", peak_value)
        print("Standard Deviation:", standard_deviation)
        print("FWHM:", FWHM)
        print("Adjusted FWHM:", adjusted_FWHM)
        print("\n\n")

if __name__ == "__main__":
    factor = 2.0

    data = SmoothDataExcludingPeaks()
    pixel_params = PixelGroupingParameters
    xtal = data.executeAlgo()
    data.PrintPeaks(xtal, pixel_params, factor)
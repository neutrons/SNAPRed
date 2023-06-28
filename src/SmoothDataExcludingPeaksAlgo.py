import json

from mantid.api import (
    AlgorithmFactory, 
    PythonAlgorithm,
)

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import PixelGroupingParametersCalculationAlgorithm
from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import PixelGroupingParameters
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import (
    name as IngestCrystallographicInfoAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


name = "SmoothDataExcludingPeaks"

@Singleton
class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        # TODO: add params
        # self.declareProperty("InputState", defaultValue="", direction=Direction.Input)


        # self.declareProperty("OutputParameters", defaultValue="", direction=Direction.Output)

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)
    
    def executeAlgo(self):
        xtalRx = self.mantidSnapper.IngestCrystallographicInfoAlgorithm(cifPath="/home/dzj/Documents/Work/Silicon_NIST_640d.cif")
        self.mantidSnapper.executeQueue()
        xtalData = xtalRx.executeRecipe("/home/dzj/Documents/Work/Silicon_NIST_640d.cif")
        xtalInfo = xtalData["crystalInfo"]

        return xtalInfo

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

    data, deldoverd = SmoothDataExcludingPeaks()
    xtal = data.executeAlgo()
    # data.PrintPeaks(xtal, deldoverd, factor)


AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)
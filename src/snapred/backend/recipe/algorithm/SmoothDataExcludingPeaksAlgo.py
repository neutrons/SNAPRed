import json

from mantid.api import (
    AlgorithmFactory, 
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.SmoothDataPeaksIngredients import SmoothDataPeaksIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


name = "SmoothDataExcludingPeaks"


class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        # TODO: add params
        self.declareProperty("Input", defaultValue="", direction=Direction.Input)
        self.declareProperty("Output", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)
    
    def PyExec(self):
        self.log().notice("Removing peaks")

        # create a workspace
        group_ws_name = "smoothpeaks_grouping"
        self.CreateGroupingWorkspace(group_ws_name)

        # load crystal data
        ingestrx = self.mantidSnapper.IngestCrystallographicInfoAlgorithm(cifPath=cifpath)

        # load data into script for manipulation
        pixel_grouping = self.mantidSnapper.PixelGroupingParametersCalculationAlgorithm()
        
        self.mantidSnapper.executeQueue()

        xtal = crystal_info["crystalInfo"]
        peaks = xtal.peaks
        Del_D_Over_D = pixel_grouping.groupingParams_str


        # xtalRx=self.mantidSnapper.IngestCrystallographicInfoAlgorithm()
        # xtalRx = self.mantidSnapper.IngestCrystallographicInfoAlgorithm(cifPath="/home/dzj/Documents/Work/Silicon_NIST_640d.cif")
        # self.mantidSnapper.executeQueue()
        # xtalData = xtalRx.executeRecipe("/home/dzj/Documents/Work/Silicon_NIST_640d.cif")
        # xtalInfo = xtalData["crystalInfo"]

        # return xtalInfo

    # def PrintPeaks(self, xtal: CrystallographicInfo, pixel_params: PixelGroupingParameters, factor):

    #     DelOverD = pixel_params.dRelativeResolution
    #     peaks = xtal.peaks
        
    #     for xtal in peaks:
    #         standard_deviation = DelOverD * xtal.dSpacing
    #         FWHM = standard_deviation * 2.35
    #         adjusted_FWHM = FWHM * factor
    #         peak_value = xtal.fSquared * xtal.multiplicity

    #     print("\n\n")
    #     print("Peak:", xtal)
    #     print("Value:", peak_value)
    #     print("Standard Deviation:", standard_deviation)
    #     print("FWHM:", FWHM)
    #     print("Adjusted FWHM:", adjusted_FWHM)
    #     print("\n\n")

AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)
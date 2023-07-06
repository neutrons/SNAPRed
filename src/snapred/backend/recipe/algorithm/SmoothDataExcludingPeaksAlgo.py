import json

from mantid.api import (
    AlgorithmFactory, 
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction
from mantid.api import *
from csaps import csaps

from snapred.backend.dao.SmoothDataPeaksIngredients import SmoothDataPeaksIngredients
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


name = "SmoothDataExcludingPeaks"


class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("SmoothDataExcludingPeaksIngredients", defaultValue = "", direction = Direction.Input)
        self.declareProperty("cifPath", defaultValue = "", direction = Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue = "", direction = Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)
    
    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        smoothpeaksIngredients = SmoothDataPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataExcludingPeaksIngredients").value)
        )

        # load a workspace
        p_ws_name = smoothpeaksIngredients.peaksWorkspace
        w_ws_name = smoothpeaksIngredients.weightsWorkspace
        p_ws = self.mantidSnapper.mtd[p_ws_name]
        numSpec = p_ws.getNumberHistograms()

        # load crystal info
        crystalInfo = smoothpeaksIngredients.crystalInfo

        # load calibration
        instrumentState = smoothpeaksIngredients.instrumentState
        
        # load CIF file
        cifPath = self.getProperty("cifPath").value
        self.mantidSnapper.LoadCIF("Loading crystal data...", Workspace=p_ws, InputFile=cifPath)

        # process crystal information
        process_crystalinfo = IngestCrystallographicInfoAlgorithm()
        process_crystalinfo.intialize()
        process_crystalinfo.setProperty("cifPath", cifPath)
        process_crystalinfo.setProperty("crystalInfo", crystalInfo.json())
        process_crystalinfo.execute()
        xtalData = json.loads(process_crystalinfo.getProperty("crystalInfo"))

        for index in range(numSpec):
            delDoD = instrumentState.pixelGroupingInstrumentParameters[index].deRelativeResolution
            

# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)


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
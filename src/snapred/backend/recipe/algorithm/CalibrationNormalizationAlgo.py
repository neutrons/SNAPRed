import json

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import (
    ReductionIngredients,
    SmoothDataExcludingPeaksIngredients,
)
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import (
    NormalizationCalibrationIngredients as Ingredients,
)
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401
from snapred.meta.Config import Config

name = "CalibrationNormalizationAlgo"


class CalibrationNormalization(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "ws_in_normalization", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "ws_background", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "ws_out_normalization", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients):
        self.run = ingredients.run
        self.backgroundRun = ingredients.backgroundRun
        self.reductionIngredients = ingredients.reductionIngredients
        self.smoothDataIngredients = ingredients.smoothDataIngredients
        self.calibrationRecord = ingredients.calibrationRecord
        self.calibrantSample = ingredients.calibrantSample
        self.calibrationWorkspace = ingredients.calibrationWorkspace
        self.isLiteRun = ingredients.run.isLite
        self.isLiteBackground = ingredients.backgroundRun.isLite
        self.focusGroup = ingredients.focusGroup
        yield

    def PyExec(self):
        ingredients = Ingredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        self.reductionIngredients.reductionState.stateConfig.focusGroups.json()
        runNumber: str = self.run.runNumber
        ipts: str = self.run.IPTS
        rawDataPath: str = ipts + "shared/SNAP_{}.nxs.h5".format(runNumber)
        backgroundRunNum: str = self.backgroundRun.runNumber
        backgroundIpts: str = self.backgroundRun.IPTS
        backgroundIpts + "shared/lite/SNAP_{}.nxs.h5".format(backgroundRunNum)

        inputWS = self.getProperty("InputWorkspace").value
        inputWSName = str(inputWS)

        backgroundWS = self.getProperty("BackgroundWorkspace").value
        backgroundWSName = str(backgroundWS)

        if self.isLiteRun is False:
            self.mantidSnapper.LoadEventNexus(
                f"Loading Nexus file for {inputWSName}...",
                Filename=rawDataPath,
                OutputWorkspace=inputWSName,
                NumberOfBins=1,
                LoadMonitors=False,
            )
            self.mantidSnapper.LiteDataCreationAlgo(
                f"Creating lite version of {inputWSName}...",
                InputWorkspace=inputWSName,
                AutoDeleteNonLiteWS=True,
                OutputWorkspace=inputWSName,
            )

        if self.isLiteBackground is False:
            self.mantidSnapper.LoadEventNexus(
                f"Loading Nexus file for {backgroundWSName}...",
                Filename=rawDataPath,
                OutputWorkspace=backgroundWSName,
                NumberOfBins=1,
                LoadMonitors=False,
            )
            self.mantidSnapper.LiteDataCreationAlgo(
                f"Creating lite version of {backgroundWSName}...",
                InputWorkspace=backgroundWSName,
                AutoDeleteNonLiteWS=True,
                OutputWorkspace=backgroundWSName,
            )

        else:
            pass

        self.mantidSnapper.executeQueue()

        self.mantidSnapper.mtd[inputWSName]
        self.mantidSnapper.mtd[backgroundWSName]

        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWSName,
            BackgroundWorkspace=backgroundWSName,
            CalibrationWorkspace=self.calibrationWorkspace.json(),
            Ingredients=self.reductionIngredients.json(),
            CalibrantSample=self.calibrantSample.json(),
            OutputWorkspace=f"{inputWSName}_raw_vanadium_data",
        )

        # TODO: Looping over grouping for customGroupWorkspace?

        self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=self.reductionIngredients.reductionState.stateConfig.json(),
            # InputWorkspace=raw_data,
            OutputWorkspace="CommonRed",
        )

        # for workspaceIndex in range(len(focusGroups)):

        # focused_data = self.mantidSnapper.DiffractionFocussing(  # GroupDetectors instead
        #     "Performing Diffraction Focusing ...",
        #     InputWorkspace= #NOTE: Will be the output of CustomGroupWorkspace,
        #     GroupingWorkspace=groupingworkspace,
        #     OutputWorkspace=f"{inputWSName}focused_data",
        #     PreserveEvents=True,
        # )

        self.mantidSnapper.RebinRagged(
            "Rebinning ragged bins...",
            # InputWorkspace=mtd[raw_data].getItem(workspaceIndex), #NOTE: Accepts single workspace (for a given group)
            # XMin=,
            # XMax=, #NOTE: XMin, XMax, etx should be lists containing all the values for the group
            #               (i.e. for column group: len(XMin) = 6)
            # Delta=focusGroups[workspaceIndex].dBin,
            # OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
            # PreserveEvents=True,
        )

        ws = f"{inputWSName}_clone_focused_data"
        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            # InputWorkspace=focused_data,
            OutputWorkspace=ws,
        )

        ws = mtd[ws]

        smooth_ws = self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            # InputWorkspace=focused_data,
            SmoothDataExcludingPeaksIngredients=self.smoothDataIngredients.json(),
            OutputWorkspace=f"{inputWS}_smooth_ws",
        )

        self.mantidSnapper.executeQueue()

        self.setProperty("FocusWorkspace", ws)
        self.setProperty("SmoothWorkspace", smooth_ws)

        return ws, smooth_ws


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)

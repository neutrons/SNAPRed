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
    NormalizationCalibrationIngredients as ingredients,
)
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401

name = "CalibrationNormalizationAlgo"


class CalibrationNormalization(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: ingredients):
        self.run = ingredients.run
        self.backgroundRun = ingredients.backgroundRun
        self.reductionIngredients = ingredients.reductionIngredients
        self.smoothDataIngredients = ingredients.smoothDataIngredients
        self.calibrationRecord = ingredients.calibrationRecord
        self.calibrantSample = ingredients.calibrantSample
        self.calibrationWorkspace = ingredients.calibrationWorkspace

    def PyExec(self):
        ingredients = ingredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        focusGroups = self.reductionIngredients.reductionState.stateConfig.focusGroups.json()
        runNumber: str = self.run.runNumber
        ipts: str = self.run.IPTS
        rawDataPath: str = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(runNumber)
        backgroundRunNum: str = self.backgroundRun.runNumber
        backgroundIpts: str = self.backgroundRun.IPTS
        backgroundRawDataPath: str = backgroundIpts + "shared/lite/SNAP_{}.lite.nxs.h5".format(backgroundRunNum)

        inputWS = self.getProperty("InputWorkspace").value
        if inputWS is None:
            self.mantidSnapper.Load(
                "Loading file for Normalization InputWS...",
                Filename=rawDataPath,
                OutputWorkspace=inputWS,
            )
        backgroundWS = self.getProperty("BackgroundWorkspace").value
        if backgroundWS is None:
            self.mantidSnapper.Load(
                "Loading file for Normalization BackgroundWS...",
                Filename=backgroundRawDataPath,
                OutputWorkspace=backgroundWS,
            )
        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        raw_data = self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWS,
            BackgroundWorkspace=backgroundWS,
            CalibrationWorkspace=self.calibrationWorkspace.json(),
            Ingredients=self.reductionIngredients.json(),
            CalibrantSample=self.calibrantSample.json(),
            OutputWorkspace="raw_data",
        )

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=self.reductionIngredients.reductionState.stateConfig.json(),
            InputWorkspace=raw_data,
            OutputWorkspace="CommonRed",
        )

        for workspaceIndex in range(len(focusGroups)):
            data = self.mantidSnapper.RebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=mtd[raw_data].getItem(workspaceIndex),
                XMin=focusGroups[workspaceIndex].dMin,
                XMax=focusGroups[workspaceIndex].dMax,
                Delta=focusGroups[workspaceIndex].dBin,
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
                PreserveEvents=False,
            )

        focused_data = self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focusing ...",
            InputWorkspace=data,
            GroupingWorkspace=groupingworkspace,
            OutputWorkspace="focused_data",
            PreserveEvents=True,
        )

        ws = "cloneFocusDataWS"
        # clone the workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace for lite data creation...",
            InputWorkspace=focused_data,
            OutputWorkspace=ws,
        )

        ws = mtd[ws]

        smooth_ws = self.mantidSnapper.SmoothDataExcludingPeaks(
            "Fit and Smooth Peaks...",
            InputWorkspace=focused_data,
            SmoothDataExcludingPeaksIngredients=self.smoothDataIngredients.json(),
            OutputWorkspace="smooth_ws",
        )

        self.mantidSnapper.executeQueue()

        self.setProperty("FocusWorkspace", "ws")
        self.setProperty("SmoothWorkspace", "smooth_ws")

        return ws, smooth_ws


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)

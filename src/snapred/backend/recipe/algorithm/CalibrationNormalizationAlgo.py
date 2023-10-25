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
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaks  # noqa F401

name = "CalibrationNormalizationAlgo"


class CalibrationNormalization(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothDataIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("BackgroundWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw vanadium background data",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Table workspace with calibration data: cols detid, difc, difa, tzero",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing corrected data; if none given, the InputWorkspace will be overwritten",
        )
        self.declareProperty("FocusWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("SmoothWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("CalibrantSample", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients.parse_raw(self.getProperty("ReductionIngredients").value)
        smoothIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataIngredients").value)
        )
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        runNumber = reductionIngredients.runConfig.runNumber
        inputWS = self.getProperty("InputWorkspace").value
        if (inputWS is None):
            self.mantidSnapper.LoadEventNexus(
                "Loading Event Nexus for Normalization InputWs...",
                Filename=
            )
        backgroundWS = self.getProperty("BackgroundWorkspace").value
        calibrationWS = self.getProperty("CalibrationWorkspace").value
        calibrantSample = self.getProperty("CalibrantSample")
        # run the algo
        self.log().notice("Execution of CalibrationNormalizationAlgo START!")

        raw_data = self.mantidSnapper.RawVanadiumCorrectionAlgorithm(
            "Correcting Vanadium Data...",
            InputWorkspace=inputWS,
            BackgroundWorkspace=backgroundWS,
            CalibrationWorkspace=calibrationWS,
            Ingredients=reductionIngredients.json(),
            CalibrantSample=calibrantSample,
            OutputWorkspace="raw_data",
        )

        groupingworkspace = self.mantidSnapper.CustomGroupWorkspace(
            "Creating Group Workspace...",
            StateConfig=reductionIngredients.reductionState.stateConfig.json(),
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
            SmoothDataExcludingPeaksIngredients=smoothIngredients.json(),
            OutputWorkspace="smooth_ws",
        )

        self.mantidSnapper.executeQueue()

        self.setProperty("FocusWorkspace", "ws")
        self.setProperty("SmoothWorkspace", "smooth_ws")

        return ws, smooth_ws


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationNormalization)

from typing import Dict, List

import numpy as np
import pydantic
from mantid.api import (
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceGroup,
    mtd,
)
from mantid.kernel import Direction, StringListValidator
from mantid.simpleapi import DeleteWorkspaces

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import allowed_peak_type_list
from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX, FitOutputEnum

logger = snapredLogger.getLogger(__name__)


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    NOYZE_2_MIN = Config["calibration.fitting.minSignal2Noise"]

    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the peaks to be fit",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Input,
            doc="Input list of peaks to be fit",
        )
        self.declareProperty(
            "PeakFunction", "Gaussian", StringListValidator(allowed_peak_type_list), direction=Direction.Input
        )
        self.declareProperty("OutputWorkspaceGroup", defaultValue="fitPeaksWSGroup", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def chopIngredients(self, ingredients: List[GroupPeakList]):
        self.groupIDs = []
        self.reducedList = {}
        for groupPeakList in ingredients:
            self.groupIDs.append(groupPeakList.groupID)
            self.reducedList[groupPeakList.groupID] = groupPeakList.peaks
        # suffixes to name diagnostic output
        self.outputSuffix = FIT_PEAK_DIAG_SUFFIX.copy()

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("Inputworkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspaceGroup")
        self.outputWorkspace = WorkspaceGroup()
        if mtd.doesExist(self.outputWorkspaceName):
            DeleteWorkspaces(list(mtd[self.outputWorkspaceName].getNames()))
        mtd.addOrReplace(self.outputWorkspaceName, self.outputWorkspace)

    def PyExec(self):
        peakFunction = self.getPropertyValue("PeakFunction")
        reducedPeakList = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(
            self.getPropertyValue("DetectorPeaks")
        )
        self.chopIngredients(reducedPeakList)
        self.unbagGroceries()

        for index, groupID in enumerate(self.groupIDs):
            tmpSpecName = mtd.unique_name(prefix=f"tmp_fitspec_{index}_")
            outputNameTmp = mtd.unique_name(prefix=f"tmp_fitdiag_{index}_")
            outputNamesTmp = {x: f"{outputNameTmp}{self.outputSuffix[x]}_{index}" for x in FitOutputEnum}

            peakCenters = []
            peakLimits = []
            for peak in self.reducedList[groupID]:
                peakCenters.append(peak.position.value)
                peakLimits.extend([peak.position.minimum, peak.position.maximum])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrum...",
                InputWorkspace=self.inputWorkspaceName,
                OutputWorkspace=tmpSpecName,
                WorkspaceIndex=index,
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                # in common with PDCalibration
                InputWorkspace=tmpSpecName,
                PeakFunction=peakFunction,
                PeakCenters=",".join(np.array(peakCenters).astype("str")),
                FitWindowBoundaryList=",".join(np.array(peakLimits).astype("str")),
                BackgroundType="Linear",
                MinimumSignalToNoiseRatio=self.NOYZE_2_MIN,
                ConstrainPeakPositions=True,
                HighBackground=True,  # vanadium must use high background
                # outputs -- in PDCalibration combined in workspace group
                FittedPeaksWorkspace=outputNamesTmp[FitOutputEnum.Workspace],
                OutputWorkspace=outputNamesTmp[FitOutputEnum.PeakPosition],
                OutputPeakParametersWorkspace=outputNamesTmp[FitOutputEnum.Parameters],
                OutputParameterFitErrorsWorkspace=outputNamesTmp[FitOutputEnum.ParameterError],
            )
            self.mantidSnapper.GroupWorkspaces(
                "Group diagnosis workspaces for output",
                InputWorkspaces=list(outputNamesTmp.values()),
                OutputWorkspace=outputNameTmp,
            )
            self.mantidSnapper.ConjoinDiagnosticWorkspaces(
                "Conjoin the diagnostic group workspaces",
                DiagnosticWorkspace=outputNameTmp,
                TotalDiagnosticWorkspace=self.outputWorkspaceName,
                AddAtIndex=index,
                AutoDelete=True,
            )
            self.mantidSnapper.WashDishes(
                "Deleting fitting workspace...",
                Workspace=tmpSpecName,
            )

        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", self.outputWorkspace.name())

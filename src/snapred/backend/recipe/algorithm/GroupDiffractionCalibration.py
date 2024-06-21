from typing import Dict, List

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.dataobjects import MaskWorkspaceProperty
from mantid.kernel import Direction, StringMandatoryValidator

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


class GroupDiffractionCalibration(PythonAlgorithm):
    """
    Calculate the group-aligned DIFC associated with a given workspace.
    One part of diffraction calibration.
    """

    NOYZE_2_MIN = Config["calibration.fitting.minSignal2Noise"]
    MAX_CHI_SQ = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("TOF"),
            ),
            doc="Workspace containing TOF neutron data.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information.",
        )
        self.declareProperty(
            ITableWorkspaceProperty("PreviousCalibrationTable", "", Direction.Input, PropertyMode.Optional),
            doc="Table workspace with previous pixel-calibrated DIFC values; if none given, will be calculated.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                "",
                Direction.Output,
                PropertyMode.Optional,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="A diffraction-focused workspace in dSpacing, after calibration constants have been adjusted.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("DiagnosticWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="A workspace group containing the fitted peaks, fit parameters, and the TOF-focused data for comparison to fits.",  # noqa E501
        )
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="if mask workspace exists: incoming mask values will be used (1.0 => dead-pixel, 0.0 => live-pixel)",
        )
        self.declareProperty(
            ITableWorkspaceProperty("FinalCalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="Table workspace with group-corrected DIFC values",
        )
        self.declareProperty(
            "Ingredients", defaultValue="", validator=StringMandatoryValidator(), direction=Direction.Input
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""

        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber

        # from grouping parameters, read the overall min/max d-spacings
        # NOTE these MUST be in order of increasing grouping ID
        # later work associating each with a group ID can relax this requirement
        self.dMin = ingredients.pixelGroup.dMin()
        self.dMax = ingredients.pixelGroup.dMax()
        self.dBin = ingredients.pixelGroup.dBin()
        pixelGroupIDs = ingredients.pixelGroup.groupIDs

        # used to be a constant pulled from application.yml
        self.maxChiSq = ingredients.maxChiSq

        # from the pixel group, read the overall min/max TOF and binning
        self.TOF = ingredients.pixelGroup.timeOfFlight

        self.peakFunction: str = ingredients.peakFunction.value
        # for each group, need a list of peaks and boundaries of peaks
        # this is found from the groupedPeakList in the ingredients
        self.groupIDs: List[int] = []
        self.groupedPeaks: Dict[int, List[float]] = {}
        self.groupedPeakBoundaries: Dict[int, List[float]] = {}
        for peakList in ingredients.groupedPeakLists:
            self.groupIDs.append(peakList.groupID)
            allPeaks: List[float] = []
            allPeakBoundaries: List[float] = []
            for peak in peakList.peaks:
                allPeaks.append(peak.value)
                allPeakBoundaries.append(peak.minimum)
                allPeakBoundaries.append(peak.maximum)
            self.groupedPeaks[peakList.groupID] = allPeaks
            self.groupedPeakBoundaries[peakList.groupID] = allPeakBoundaries
        # NOTE: this sort is necessary to correspond to the sort inside mantid's GroupingWorkspace
        self.groupIDs = sorted(self.groupIDs)
        # TODO put in a validateInputs method
        assert self.groupIDs == pixelGroupIDs

        if len(self.groupIDs) != len(ingredients.pixelGroup.pixelGroupingParameters):
            raise RuntimeError(
                f"Group IDs do not match between peak list and focus group: {self.groupIDs} vs {len(ingredients.pixelGroup.pixelGroupingParameters)}"  # noqa: E501
            )

        self.diagnosticSuffix = [None] * (len(FitOutputEnum) - 1)
        self.diagnosticSuffix[FitOutputEnum.PeakPosition.value] = "_dspacing"
        self.diagnosticSuffix[FitOutputEnum.Parameters.value] = "_fitparam"
        self.diagnosticSuffix[FitOutputEnum.Workspace.value] = "_fitted"
        # self.diagnosticSuffix[FitOutputEnum.ParameterError.value] = "_fiterror"

    def unbagGroceries(self):
        """
        Process input neutron data
        """

        self.originalWStof: str = self.getPropertyValue("InputWorkspace")
        self.focusWS: str = str(self.getPropertyValue("GroupingWorkspace"))

        # create string names of workspaces that will be used by algorithm
        self.outputWStof: str = wng.diffCalOutput().runNumber(self.runNumber).build()
        self.diagnosticWS: str = ""
        if self.getProperty("DiagnosticWorkspace").isDefault:
            self.diagnosticWS = f"group_diffcal_{self.runNumber}_diagnostic"
            self.setPropertyValue("DiagnosticWorkspace", self.diagnosticWS)
        else:
            self.diagnosticWS = self.getPropertyValue("DiagnosticWorkspace")

        self.outputWSdSpacing: str = ""
        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWSdSpacing = wng.diffCalOutputdSpacing().runNumber(self.runNumber).build()
            self.setPropertyValue("OutputWorkspace", self.outputWSdSpacing)
        else:
            self.outputWSdSpacing = self.getPropertyValue("OutputWorkspace")

        self.maskWS: str = ""
        if self.getProperty("MaskWorkspace").isDefault:
            self.maskWS = wng.diffCalMask().runNumber(self.runNumber).build()
            self.setProperty("MaskWorkspace", self.maskWS)
        else:
            self.maskWS = self.getPropertyValue("MaskWorkspace")

        # set the previous calibration table, or create if none given
        # TODO: use workspace namer
        self.DIFCprev: str = ""
        if self.getProperty("PreviousCalibrationTable").isDefault:
            self.DIFCprev = f"diffract_consts_prev_{self.runNumber}"
            self.mantidSnapper.CalculateDiffCalTable(
                "Initialize the DIFC table from input",
                InputWorkspace=self.originalWStof,
                CalibrationTable=self.DIFCprev,
                OffsetMode="Signed",
                BinWidth=self.TOF.binWidth,
            )
        else:
            self.DIFCprev = self.getPropertyValue("PreviousCalibrationTable")

        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration table to the input workspace",
            InstrumentWorkspace=self.originalWStof,
            CalibrationWorkspace=self.DIFCprev,
        )

        # set the final calibration table, to be the output
        self.DIFCfinal: str = ""
        if self.getProperty("FinalCalibrationTable").isDefault:
            self.DIFCfinal = self.DIFCprev
            self.setProperty("FinalCalibrationTable", self.DIFCfinal)
        else:
            self.DIFCfinal = str(self.getPropertyValue("PreviousCalibrationTable"))
            self.mantidSnapper.CloneWorkspace(
                "Make copy of previous calibration table to use as first input",
                InputWorkspace=self.DIFCprev,
                OutputWorkspace=self.DIFCfinal,
            )
        self.mantidSnapper.MakeDirtyDish(
            "Make a copy of initial DIFC prev",
            InputWorkspace=self.DIFCprev,
            OutputWorkspace=self.DIFCprev + "_before",
        )

        # process and diffraction focus the input data
        # must convert to d-spacing, diffraction focus, ragged rebin, then convert back to TOF
        self.convertAndFocusAndReturn(self.originalWStof, self.outputWStof, "before", "TOF")

    def verifyChiSq(self, diagnosticWSname):
        diagnosticWS = self.mantidSnapper.mtd[diagnosticWSname]
        tab = self.mantidSnapper.mtd[diagnosticWS.getNames()[0]]
        tabDict = tab.toDict()
        chi2 = tabDict["chisq"]
        totalLowChi2 = 0
        badPeaks = []
        if len(chi2) > 2:
            for index, item in enumerate(chi2):
                if item < self.maxChiSq:
                    totalLowChi2 = totalLowChi2 + 1
                else:
                    badPeaks.append(
                        {
                            "Spectrum": tabDict["wsindex"][index],
                            "Peak Location": tabDict["centre"][index],
                            "Chi2": tabDict["chi2"][index],
                        }
                    )
            if totalLowChi2 < 2:
                logger.warning(
                    f"Insufficient number of well-fitted peaks (chi2 < {self.maxChiSq})."
                    + "Try to adjust parameters in Tweak Peak Peek tab"
                    + f"Bad peaks info: {badPeaks}"
                )
            else:
                logger.info(f"Sufficient number of well-fitted peaks (chi2 < {self.maxChiSq}).: {totalLowChi2}")

    def PyExec(self) -> None:
        """
        Execute the group-by-group calibration algorithm.
        First a table of previous pixel calibrations must be loaded.
        Then, group-by-group, each spectrum is calibrated by fitting peaks, and the
        resulting DIFCs are combined with the previous table.
        The final calibration table is saved to disk for future use.
        input:
        - Ingredients: DiffractionCalibrationIngredients -- the DAO holding data needed to run the algorithm
        - InputWorkspace: str -- the name of workspace holding the initial TOF data
        - PreviousCalibrationTable: str -- the name of the table workspace with previous DIFC values
        output:
        - OutputWorkspace: str -- the name of the diffraction-focussed d-spacing data after the final calibration
        - MaskWorkspace: str -- the name of the mask workspace for detectors failing calibration
        (1.0 => dead-pixel, 0.0 => live-pixel)
        when the mask workspace already exists, the incoming pixel mask will be combined
        with any new masked pixels detected during execution
        - FinalCalibrationTable: str -- the name of the final table of DIFC values
        """

        # run the algo
        self.log().notice("Execution of group diffraction calibration START!")

        # get the ingredients
        ingredients = Ingredients.model_validate_json(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries()

        diffocWS = self.mantidSnapper.mtd[self.outputWStof]
        nHist = diffocWS.getNumberHistograms()
        if nHist != len(self.groupIDs):
            raise RuntimeError("error, the number of spectra in focused workspace, and number of groups, do not match")

        for index in range(nHist):
            groupID: int = self.groupIDs[index]
            DIFCpd: str = f"_tmp_DIFCgroup_{groupID}"
            diagnosticWSgroup: str = f"_pdcal_diag_{groupID}"
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on group {groupID}",
                # in common with FitPeaks
                InputWorkspace=self.outputWStof,
                PeakFunction=self.peakFunction,
                PeakPositions=self.groupedPeaks[groupID],
                PeakWindow=self.groupedPeakBoundaries[groupID],
                BackgroundType="Linear",
                MinimumSignalToNoiseRatio=self.NOYZE_2_MIN,
                ConstrainPeakPositions=True,
                HighBackground=True,  # vanadium must use high background to FitPeaks
                # output -- in FitPeaks listed as four workspaces
                DiagnosticWorkspaces=diagnosticWSgroup,
                # specific to PDCalibration
                TofBinning=self.TOF.params,
                MaxChiSq=self.maxChiSq,
                CalibrationParameters="DIFC",
                OutputCalibrationTable=DIFCpd,
                MaskWorkspace=self.maskWS,
                # limit to specific spectrum
                StartWorkspaceIndex=index,
                StopWorkspaceIndex=index,
            )
            self.conjoinDiagnosticWorkspaces(index, self.diagnosticWS, diagnosticWSgroup)
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.DIFCprev,  # previous calibration values, DIFCprev
                GroupedCalibration=DIFCpd,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.outputWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.DIFCfinal,  # resulting corrected calibration values, DIFCeff
            )
            # use the corrected workspace as starting point of next iteration
            self.mantidSnapper.MakeDirtyDish(
                f"Create record of how DIFC looks at group {index}",
                InputWorkspace=self.DIFCprev,
                OutputWorkspace=self.DIFCprev + f"_{index}",
            )
            if self.DIFCfinal != self.DIFCprev:
                self.mantidSnapper.RenameWorkspace(
                    "Use the corrected diffcal workspace as starting point in next iteration",
                    InputWorkspace=self.DIFCfinal,
                    OutputWorkspace=self.DIFCprev,
                )
            self.mantidSnapper.executeQueue()
            self.verifyChiSq(self.diagnosticWS)
            self.mantidSnapper.WashDishes(
                "Cleanup leftover workspaces",
                WorkspaceList=[DIFCpd],
            )

        # apply the calibration table to input data, then re-focus
        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.originalWStof,
            CalibrationWorkspace=self.DIFCfinal,
        )
        self.convertAndFocusAndReturn(self.originalWStof, self.outputWSdSpacing, "after", "dSpacing")

        # add the TOF-spacing workspace to the diagnostic workspace group
        self.mantidSnapper.CloneWorkspace(
            "Clone the final output that is in dSpacing units",
            InputWorkspace=self.outputWSdSpacing,
            OutputWorkspace=self.outputWStof,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert the clone of the final output back to TOF",
            InputWorkspace=self.outputWStof,
            OutputWorkspace=self.outputWStof,
            Target="TOF",
        )
        self.mantidSnapper.executeQueue()
        diagnostic = self.mantidSnapper.mtd[self.diagnosticWS]
        diagnostic.add(self.outputWStof)

        # set the outputs
        self.setPropertyValue("DiagnosticWorkspace", self.diagnosticWS)
        self.setPropertyValue("OutputWorkspace", self.outputWSdSpacing)

    def convertAndFocusAndReturn(self, inputWS: str, outputWS: str, note: str, units: str):
        # Use workspace name generator
        tmpWStof = f"_TOF_{self.runNumber}_diffoc_{note}"
        tmpWSdsp = f"_DSP_{self.runNumber}_diffoc_{note}"

        # Convert the raw TOF data to d-spacing
        self.mantidSnapper.ConvertUnits(
            "Convert the raw TOF data to d-spacing",
            InputWorkspace=inputWS,
            OutputWorkspace=tmpWSdsp,
            Target="dSpacing",
        )

        # Diffraction-focus the d-spacing data
        self.mantidSnapper.DiffractionFocussing(
            "Diffraction-focus the d-spacing data",
            InputWorkspace=tmpWSdsp,
            GroupingWorkspace=self.focusWS,
            OutputWorkspace=tmpWSdsp,
        )

        # Ragged rebin the diffraction-focussed data
        self.mantidSnapper.RebinRagged(
            "Ragged rebin the diffraction-focussed data",
            InputWorkspace=tmpWSdsp,
            OutputWorkspace=tmpWSdsp,
            XMin=self.dMin,
            XMax=self.dMax,
            Delta=self.dBin,
        )

        if units == "TOF":
            # Convert the focussed and rebinned data back to TOF if required
            self.mantidSnapper.ConvertUnits(
                "Convert the focussed and rebinned data back to TOF",
                InputWorkspace=tmpWSdsp,
                OutputWorkspace=outputWS,
                Target="TOF",
            )

            # For inspection, save diffraction focused data before calculation
            self.mantidSnapper.MakeDirtyDish(
                "Save diffraction-focused TOF data",
                InputWorkspace=outputWS if units == "TOF" else tmpWSdsp,
                OutputWorkspace=tmpWStof,
            )

            # Delete diffraction-focused d-spacing data
            self.mantidSnapper.WashDishes(
                "Delete diffraction-focused d-spacing data",
                Workspace=tmpWSdsp,
            )
        else:
            self.mantidSnapper.RenameWorkspace(
                "",
                InputWorkspace=tmpWSdsp,
                OutputWorkspace=outputWS,
            )

        # Execute queued Mantid algorithms
        self.mantidSnapper.executeQueue()

    def conjoinDiagnosticWorkspaces(self, index, diagnosticWS, diagnosticWStmp):  # noqa: ARG002
        # on first index, clone the diagnostic workspace group
        self.mantidSnapper.UnGroupWorkspace(
            "Ungroup the temp daignostic workspaces",
            InputWorkspace=diagnosticWStmp,
        )
        self.mantidSnapper.ExtractSingleSpectrum(
            "Remove the indicated spectrum",
            InputWorkspace=f"{diagnosticWStmp}{self.diagnosticSuffix[FitOutputEnum.Workspace.value]}",
            Outputworkspace=f"{diagnosticWStmp}{self.diagnosticSuffix[FitOutputEnum.Workspace.value]}",
            WorkspaceIndex=index,
        )
        if index == 0:
            oldNames = [f"{diagnosticWStmp}{suffix}" for suffix in self.diagnosticSuffix]
            newNames = [f"{diagnosticWS}{suffix}" for suffix in self.diagnosticSuffix]
            self.mantidSnapper.RenameWorkspaces(
                "Rename the diagnostic workspaces in the group",
                InputWorkspaces=oldNames,
                WorkspaceNames=newNames,
            )
            self.mantidSnapper.GroupWorkspaces(
                "Create a new workspace group",
                InputWorkspaces=newNames,
                OutputWorkspace=diagnosticWS,
            )
        else:
            # combine the matrix workspaces
            for x in [FitOutputEnum.Workspace.value]:
                self.mantidSnapper.ConjoinWorkspaces(
                    "Conjoin peak position workspaces",
                    InputWorkspace1=f"{diagnosticWS}{self.diagnosticSuffix[x]}",
                    InputWorkspace2=f"{diagnosticWStmp}{self.diagnosticSuffix[x]}",
                    CheckOverlapping=False,
                )
                self.mantidSnapper.WashDishes(
                    "Clear temporary workspace",
                    WorkspaceList=[f"{diagnosticWStmp}{self.diagnosticSuffix[x]}"],
                )
            # combine the table workspaces
            for x in [FitOutputEnum.Parameters.value, FitOutputEnum.PeakPosition.value]:
                self.mantidSnapper.ConjoinTableWorkspaces(
                    "Conjoin peak fit parameter workspaces",
                    InputWorkspace1=f"{diagnosticWS}{self.diagnosticSuffix[x]}",
                    InputWorkspace2=f"{diagnosticWStmp}{self.diagnosticSuffix[x]}",
                    AutoDelete=True,
                )
        self.mantidSnapper.WashDishes(
            "Remove the temporary diagnostic group",
            WorkspaceList=[f"{diagnosticWStmp}{x}" for x in ["", "_height", "_width", "_resolution", "_dspacing"]],
        )


AlgorithmFactory.subscribe(GroupDiffractionCalibration)

from typing import Dict, List, Set

from pydantic import BaseModel

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.profiling.ProgressRecorder import ComputationalOrder, WallClockTime
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX, FitOutputEnum
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


class GroupDiffCalServing(BaseModel):
    result: bool
    diagnosticWorkspace: str
    outputWorkspace: str
    calibrationTable: str
    maskWorkspace: str


# Decorating with the `WallClockTime` profiler here somewhat duplicates the objective of the decoration
#   at `CalibrationService.groupCalibration`.  However, by default, there will be no logging output from this instance.
#   This duplication allows both verification of this alternative decorator signature, and comparison of the
#   profiling measurements and estimate initialization from the two instances.
@WallClockTime(callerOverride="cook", N_ref="_groupDiffCal_N_ref", order=ComputationalOrder.O_N)
class GroupDiffCalRecipe(Recipe[Ingredients]):
    """
    Calculate the group-aligned DIFC associated with a given workspace.
    One part of diffraction calibration.
    """

    def __init__(self, utensils: Utensils = None):
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper
        self._counts = 0

    @classproperty
    def NOYZE_2_MIN(cls):
        return Config["calibration.fitting.minSignal2Noise"]

    @classproperty
    def MAX_CHI_SQ(cls):
        return Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    def logger(self):
        return logger

    def allGroceryKeys(self) -> Set[str]:
        return {
            "inputWorkspace",
            "groupingWorkspace",
            "maskWorkspace",
            "outputWorkspace",
            "diagnosticWorkspace",
            "previousCalibration",
            "calibrationTable",
        }

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace", "groupingWorkspace"}

    def validateInputs(self, ingredients: Ingredients, groceries: Dict[str, WorkspaceName]):
        super().validateInputs(ingredients, groceries)

        pixelGroupIDs = ingredients.pixelGroup.groupIDs
        groupIDs = [peakList.groupID for peakList in ingredients.groupedPeakLists]
        if groupIDs != pixelGroupIDs:
            raise RuntimeError(
                f"Group IDs do not match between peak list and the pixel group: '{groupIDs}' vs. '{pixelGroupIDs}'"
            )

        diffocWS = self.mantidSnapper.mtd[groceries["groupingWorkspace"]]
        if groupIDs != diffocWS.getGroupIDs().tolist():
            raise RuntimeError(
                f"Group IDs do not match between peak list and focus WS: '{groupIDs}' vs. '{diffocWS.getGroupIDs()}'"
            )

    def chopIngredients(self, ingredients: Ingredients) -> None:
        """Process the needed ingredients for use in recipe"""
        self.runNumber: str = ingredients.runConfig.runNumber

        # from grouping parameters, read the overall min/max d-spacings
        self.pixelGroup = ingredients.pixelGroup

        self.grouping = ingredients.pixelGroup.focusGroup.name

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

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]) -> None:
        """
        Process input neutron data
        """
        logger.debug(f"Groceries: {groceries}")
        self.diagnosticSuffix = FIT_PEAK_DIAG_SUFFIX.copy()

        self.originalWStof = groceries["inputWorkspace"]
        self.focusWS = groceries["groupingWorkspace"]
        self.outputWStof = wng.diffCalOutput().runNumber(self.runNumber).build()
        self.outputWStofFocused = wng.diffCalOutput().runNumber(self.runNumber).group(self.grouping).build()
        self.outputWSdSpacing = groceries.get(
            "outputWorkspace", wng.diffCalOutput().runNumber(self.runNumber).unit("DSP").build()
        )
        self.diagnosticWS = groceries.get("diagnosticWorkspace", f"group_diffcal_{self.runNumber}_diagnostic")

        self.maskWS = groceries.get("maskWorkspace", wng.diffCalMask().runNumber(self.runNumber).build())

        # set the previous calibration table, or create if none given
        # TODO: use workspace namer
        DIFCprev: str = groceries.get("previousCalibration", "")
        if DIFCprev == "":
            DIFCprev = f"diffract_consts_prev_{self.runNumber}"
            self.mantidSnapper.CalculateDiffCalTable(
                "Initialize the DIFC table from input",
                InputWorkspace=self.originalWStof,
                CalibrationTable=DIFCprev,
                OffsetMode="Signed",
                BinWidth=self.TOF.binWidth,
            )

        # set the final calibration table, to be the output
        self.DIFCfinal: str = groceries.get("calibrationTable", DIFCprev)
        if self.DIFCfinal != DIFCprev:
            self.mantidSnapper.CloneWorkspace(
                "Make copy of previous calibration table to use as first input",
                InputWorkspace=DIFCprev,
                OutputWorkspace=self.DIFCfinal,
            )

        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration table to the input workspace",
            InstrumentWorkspace=self.originalWStof,
            CalibrationWorkspace=self.DIFCfinal,
        )
        self.mantidSnapper.MakeDirtyDish(
            "Make a copy of initial DIFC prev",
            InputWorkspace=self.DIFCfinal,
            OutputWorkspace=self.DIFCfinal + "_before",
        )

        # process and diffraction focus the input data
        # must convert to d-spacing, diffraction focus, ragged rebin, then convert back to TOF
        self.convertAndFocusAndReturn(self.originalWStof, self.outputWStof, "before", "TOF")

    def prep(self, ingredients: Ingredients, groceries: Dict[str, str]):
        """
        Convenience method to prepare the recipe for execution.
        """
        self.validateInputs(ingredients, groceries)
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)
        self.stirInputs()

        self.queueAlgos()

    def verifyChiSq(self, diagnosticWSName):
        peakFitParamWSName = f"{diagnosticWSName}{self.diagnosticSuffix[FitOutputEnum.Parameters]}"
        self.mantidSnapper.VerifyChiSquared(
            "Get the lists of good and bad peaks",
            InputWorkspace=peakFitParamWSName,
            MaximumChiSquared=self.maxChiSq,
            LogResults=True,
        )

    def queueAlgos(self):
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

        for index in range(len(self.groupIDs)):
            groupID: int = self.groupIDs[index]
            DIFCpd: str = f"__tmp_DIFCgroup_{groupID}"
            diagnosticWSgroup: str = f"__pdcal_diag_{groupID}"
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
            self.mantidSnapper.ConjoinDiagnosticWorkspaces(
                "Combine the diagnostic outputs",
                DiagnosticWorkspace=diagnosticWSgroup,
                TotalDiagnosticWorkspace=self.diagnosticWS,
                AddAtIndex=index,
                AutoDelete=True,
            )
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.DIFCfinal,  # previous calibration values, DIFCprev
                GroupedCalibration=DIFCpd,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.outputWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.DIFCfinal,  # resulting corrected calibration values, DIFCeff
            )
            # use the corrected workspace as starting point of next iteration
            self.mantidSnapper.MakeDirtyDish(
                f"Create record of how DIFC looks at group {index}",
                InputWorkspace=self.DIFCfinal,
                OutputWorkspace=self.DIFCfinal + f"_{index}",
            )
            self.mantidSnapper.WashDishes(
                "Cleanup leftover workspaces",
                WorkspaceList=[DIFCpd],
            )
            self.verifyChiSq(self.diagnosticWS)

        # apply the calibration table to input data, then re-focus
        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.originalWStof,
            CalibrationWorkspace=self.DIFCfinal,
        )
        self.convertAndFocusAndReturn(self.originalWStof, self.outputWSdSpacing, "after", "dSpacing", False)

        # add the TOF-spacing workspace to the diagnostic workspace group
        self.mantidSnapper.CloneWorkspace(
            "Clone the final output that is in dSpacing units",
            InputWorkspace=self.outputWSdSpacing,
            OutputWorkspace=self.outputWStof,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert the clone of the final output back to TOF",
            InputWorkspace=self.outputWStof,
            OutputWorkspace=self.outputWStofFocused,
            Target="TOF",
        )
        self.mantidSnapper.DeleteWorkspace(
            "Deleting unused workspace",
            Workspace=self.outputWStof,
        )

    def execute(self):
        self.mantidSnapper.executeQueue()
        diagnostic = self.mantidSnapper.mtd[self.diagnosticWS]
        diagnostic.add(self.outputWStofFocused)
        for ws in diagnostic.getNames():
            if self.diagnosticSuffix[FitOutputEnum.PeakPosition] in ws:
                self.mantidSnapper.DeleteWorkspace(
                    "Deleting unused workspace",
                    Workspace=ws,
                )
                self.mantidSnapper.executeQueue()
                break

        self.mantidSnapper.executeQueue()

    def _groupDiffCal_N_ref(self, _ingredients: Ingredients, groceries: Dict[str, str]) -> float | None:
        # Calculate the reference value to use during the estimation of execution time for the
        # `groupDiffCalRecipe.cook` method.

        # -- This method must have the same calling signature as `cook`.
        # -- Returning `None` means that the `N_ref` value cannot be calculated, or alternatively,
        #    that it should not be used.
        # -- Please see:
        #    `snapred.readthedocs.io/en/latest/developer/implementation_notes/profiling_and_progress_recording.html`.

        # This method is largely the same as `CalibrationService._calibration_substep_N_ref`, however, by
        #   declaring it here it can be modified separately, depending on the requirements when more profiling data
        #   becomes available.

        inputWorkspace = groceries["inputWorkspace"]
        N_ref = None
        if self.mantidSnapper.mtd.doesExist(inputWorkspace):
            # Notes:
            #   -- Scaling probably also has a sub-linear dependence on the number of subgroups in the grouping,
            #      however, that should be accounted for during the estimator's spline fit.
            #   -- `getMemorySize` appears to return bytes (, which contradicts what the Mantid docs say).
            #      Note that the default estimator instance is designed assuming its `N_ref` input will be in bytes.
            #      It's important that these match, so that the transition between the default estimator,
            #      and the first updated estimator will be as transparent as possible.
            dataSize = float(self.mantidSnapper.mtd[inputWorkspace].getMemorySize())
            N_ref = dataSize

        return N_ref

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> GroupDiffCalServing:
        self.prep(ingredients, groceries)
        self.execute()
        # set the outputs
        return GroupDiffCalServing(
            result=True,
            diagnosticWorkspace=self.diagnosticWS,
            outputWorkspace=self.outputWSdSpacing,
            calibrationTable=self.DIFCfinal,
            maskWorkspace=self.maskWS,
        )

    def convertAndFocusAndReturn(self, inputWS: str, outputWS: str, note: str, units: str, keepEvents: bool = True):
        # TODO use workspace name generator
        tmpWStof = f"_TOF_{self.runNumber}_diffoc_{note}"
        tmpWSdsp = f"_DSP_{self.runNumber}_diffoc_{note}"

        # Convert the raw TOF data to d-spacing
        self.mantidSnapper.ConvertUnits(
            "Convert the raw TOF data to d-spacing",
            InputWorkspace=inputWS,
            OutputWorkspace=tmpWSdsp,
            Target="dSpacing",
        )

        # Diffraction focus, and rebin the d-spacing data.
        self.mantidSnapper.FocusSpectraAlgorithm(
            "Diffraction-focus the d-spacing data",
            InputWorkspace=tmpWSdsp,
            GroupingWorkspace=self.focusWS,
            OutputWorkspace=tmpWSdsp,
            PixelGroup=self.pixelGroup.model_dump_json(),
            PreserveEvents=keepEvents,
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

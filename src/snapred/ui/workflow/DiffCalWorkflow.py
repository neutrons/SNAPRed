from qtpy.QtCore import Slot

from snapred.backend.dao import RunConfig
from snapred.backend.dao.Limit import Pair
from snapred.backend.dao.request import (
    CalculateResidualRequest,
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    CalibrationWritePermissionsRequest,
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    DiffractionCalibrationRequest,
    FitMultiplePeaksRequest,
    FocusSpectraRequest,
    HasStateRequest,
    OverrideRequest,
    RunMetadataRequest,
    SimpleDiffCalRequest,
)
from snapred.backend.dao.request.RenameWorkspaceRequest import RenameWorkspaceRequest
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceType as wngt,
)
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView
from snapred.ui.view.DiffCalRequestView import DiffCalRequestView
from snapred.ui.view.DiffCalSaveView import DiffCalSaveView
from snapred.ui.view.DiffCalTweakPeakView import DiffCalTweakPeakView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class DiffCalWorkflow(WorkflowImplementer):
    """

    The DiffCalWorkflow class orchestrates a comprehensive process for diffraction calibration in SNAPRed,
    starting from calibration request setup to the final decision on saving the calibration results. It
    leverages a series of interconnected views (DiffCalRequestView, DiffCalTweakPeakView,
    DiffCalAssessmentView, DiffCalSaveView) to guide users through each step of calibration, including
    parameter input, peak adjustment, calibration assessment, and optional data saving.

    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.mantidSnapper = MantidSnapper(None, "Utensils")
        # create a tree of flows for the user to successfully execute diffraction calibration
        # DiffCal Request ->
        # Check Peaks     ->
        # Calibrate       ->
        # Assess          ->
        # Save?           ->
        self.samplePaths = self.request(path="config/samplePaths").data
        self.defaultGroupingMap = self.request(path="config/groupingMap", payload="tmfinr").data
        self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.lite
        self.removeBackground = False

        self.addResetHook(self._resetSaveView)

        self._requestView = DiffCalRequestView(
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._tweakPeakView = DiffCalTweakPeakView(
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._assessmentView = DiffCalAssessmentView(
            parent=parent,
        )
        self._saveView = DiffCalSaveView(parent)

        # connect signal to populate the grouping dropdown after run is selected
        self._requestView.liteModeToggle.stateChanged.connect(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._requestView.sampleDropdown.dropDown.currentIndexChanged.connect(self._lookForOverrides)
        self._tweakPeakView.signalValueChanged.connect(self.onValueChange)
        self._tweakPeakView.signalPurgeBadPeaks.connect(self.purgeBadPeaks)

        # connect the lite mode toggles across the views
        self._requestView.liteModeToggle.stateChanged.connect(self._tweakPeakView.liteModeToggle.setState)
        self._tweakPeakView.liteModeToggle.stateChanged.connect(self._requestView.liteModeToggle.setState)

        # connect the skip pixelcal toggles across the views
        self._requestView.skipPixelCalToggle.stateChanged.connect(self._tweakPeakView.skipPixelCalToggle.setState)
        self._tweakPeakView.skipPixelCalToggle.stateChanged.connect(self._requestView.skipPixelCalToggle.setState)

        self.prevFWHM = DiffCalTweakPeakView.default_FWHM
        self.prevXtalDMin = DiffCalTweakPeakView.default_XTAL_DMIN
        self.prevXtalDMax = DiffCalTweakPeakView.default_XTAL_DMAX

        self.peaksWerePurged = False

        # 1. input run number and other basic parameters
        # 2. display peak graphs, allow adjustments to view
        # 3. perform diffraction calibration after user approves peaks then move on
        # 4. user assesses calibration and chooses to iterate, or continue
        # 5. user may elect to save the calibration
        self.workflow = (
            WorkflowBuilder(
                startLambda=self.start,
                iterateLambda=self.iterate,
                # Any implicit reset will retain output workspaces (at present: meaning reduction-output only).
                resetLambda=self.reset,
                parent=parent,
            )
            .addNode(
                self._specifyRun,
                self._requestView,
                "Diffraction Calibration",
                continueAnywayHandler=self._continueAnywayHandler,
            )
            .addNode(
                self._triggerDiffractionCalibration,
                self._tweakPeakView,
                "Tweak Peak Peek",
                continueAnywayHandler=self._continueAnywayHandlerTweak,
            )
            .addNode(self._assessCalibration, self._assessmentView, "Assessing", iterate=True)
            .addNode(self._saveCalibration, self._saveView, name="Saving")
            .build()
        )

    @classproperty
    def DEFAULT_DMIN(cls):
        return Config["constants.CrystallographicInfo.crystalDMin"]

    @classproperty
    def DEFAULT_DMAX(cls):
        return Config["constants.CrystallographicInfo.crystalDMax"]

    @classproperty
    def DEFAULT_NBINS(cls):
        return Config["calibration.diffraction.nBinsAcrossPeakWidth"]

    @classproperty
    def DEFAULT_CONV(cls):
        return Config["calibration.diffraction.convergenceThreshold"]

    @classproperty
    def DEFAULT_MAX_CHI_SQ(cls):
        return Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    def _continueAnywayHandlerTweak(self, continueInfo: ContinueWarning.Model):  # noqa: ARG002
        self._continueAnywayHandler(continueInfo)
        self._tweakPeakView.updateContinueAnyway(True)

    def __setInteraction(self, state: bool, interactionType: str):
        if interactionType == "populateGroupDropdown":
            self._requestView.liteModeToggle.setEnabled(state)
            self._requestView.skipPixelCalToggle.setEnabled(state)
            self._requestView.groupingFileDropdown.setEnabled(state)
        elif interactionType == "lookForOverrides":
            self._requestView.sampleDropdown.setEnabled(state)

    @ExceptionToErrLog
    @Slot()
    def _populateGroupingDropdown(self):
        # when the run number is updated, freeze the drop down to populate it
        runNumber = self._requestView.runNumberField.text()
        useLiteMode = self._requestView.liteModeToggle.getState()

        self.__setInteraction(False, "populateGroupDropdown")

        def _onDropdownSuccess():
            self.__setInteraction(True, "populateGroupDropdown")
            self._populateRunMetadata(runNumber)

        self.workflow.presenter.handleAction(
            self.handleDropdown,
            args=(runNumber, useLiteMode),
            onSuccess=_onDropdownSuccess,
        )

    @ExceptionToErrLog
    @Slot()
    def _populateRunMetadata(self, runNumber: str):
        if not bool(runNumber):
            self._requestView.updateRunMetadata()
            return
        self.workflow.presenter.handleAction(
            self.handleRunMetadata,
            args=(runNumber,),
            onSuccess=lambda: self.__setInteraction(False, "populateRunMetadata"),
        )

    @ExceptionToErrLog
    @Slot()
    def _lookForOverrides(self):
        sampleFile = self._requestView.sampleDropdown.currentText()
        if not sampleFile:
            return
        self.__setInteraction(False, "lookForOverrides")
        self.workflow.presenter.handleAction(
            self.handleOverride,
            args=(sampleFile),
            onSuccess=lambda: self.__setInteraction(True, "lookForOverrides"),
        )

    def handleDropdown(self, runNumber, useLiteMode):
        # check if the state exists -- if so load its grouping map
        payload = HasStateRequest(
            runId=runNumber,
            useLiteMode=useLiteMode,
        )
        hasState = self.request(path="calibration/hasState", payload=payload.json()).data
        if hasState:
            self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
        else:
            self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(useLiteMode)

        # populate and re-enable the drop down
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        return SNAPResponse(code=ResponseCode.OK)

    def handleRunMetadata(self, runNumber):
        payload = RunMetadataRequest(runId=runNumber)
        metadata = self.request(path="calibration/runMetadata", payload=payload.json()).data
        self._requestView.updateRunMetadata(metadata)
        return SNAPResponse(code=ResponseCode.OK)

    def handleOverride(self, sampleFile):
        payload = OverrideRequest(calibrantSamplePath=sampleFile)
        overrides = self.request(path="calibration/override", payload=payload.json()).data

        if not overrides:
            self._requestView.enablePeakFunction()
            self._tweakPeakView.enablePeakFunction()

            self._tweakPeakView.updateXtalDmin(self.prevXtalDMin)
            self._tweakPeakView.enableXtalDMin()
            self._tweakPeakView.updateXtalDmax(self.prevXtalDMax)
            self._tweakPeakView.enableXtalDMax()

            return SNAPResponse(code=ResponseCode.OK)

        if "peakFunction" in overrides:
            peakFunction = overrides["peakFunction"]

            reqComboBox = self._requestView.peakFunctionDropdown.dropDown
            idxRQ = reqComboBox.findText(peakFunction)
            if idxRQ >= 0:
                reqComboBox.setCurrentIndex(idxRQ)
            self._requestView.disablePeakFunction()

            twkComboBox = self._tweakPeakView.peakFunctionDropdown.dropDown
            idxTW = twkComboBox.findText(peakFunction)
            if idxTW >= 0:
                twkComboBox.setCurrentIndex(idxTW)
            self._tweakPeakView.disablePeakFunction()

        if "crystalDMin" in overrides:
            newDMin = overrides["crystalDMin"]
            self._tweakPeakView.updateXtalDmin(newDMin)
            self._tweakPeakView.disableXtalDMin()
            self.prevXtalDMin = newDMin
        else:
            self._tweakPeakView.updateXtalDmin(self.prevXtalDMin)
            self._tweakPeakView.enableXtalDMin()

        if "crystalDMax" in overrides:
            newDMax = overrides["crystalDMax"]
            self._tweakPeakView.updateXtalDmax(newDMax)
            self._tweakPeakView.disableXtalDMax()
            self.prevXtalDMax = newDMax
        else:
            self._tweakPeakView.updateXtalDmax(self.prevXtalDMax)
            self._tweakPeakView.enableXtalDMax()

        return SNAPResponse(code=ResponseCode.OK)

    @ExceptionToErrLog
    @Slot()
    def _switchLiteNativeGroups(self):
        # determine resolution mode
        useLiteMode = self._requestView.liteModeToggle.getState()

        # set default state for skipPixelCalToggle
        # in native mode, skip by default
        # in lite mode, do not skip by default
        self._requestView.skipPixelCalToggle.setState(not useLiteMode)

        self._requestView.groupingFileDropdown.setEnabled(False)

        self.workflow.presenter.handleAction(
            self.handleSwitchLiteNative,
            args=(useLiteMode,),
            onSuccess=lambda: self._requestView.groupingFileDropdown.setEnabled(True),
        )

    def handleSwitchLiteNative(self, useLiteMode):
        self.focusGroups = self.groupingMap.getMap(useLiteMode)
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        return SNAPResponse(code=ResponseCode.OK)

    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _specifyRun(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        # fetch the data from the view
        self.runNumber = view.runNumberField.text()
        self.useLiteMode = view.liteModeToggle.getState()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.calibrantSamplePath = view.sampleDropdown.currentText()
        self.peakFunction = view.peakFunctionDropdown.currentText()
        self.skipPixelCal = view.getSkipPixelCalibration()
        self.maxChiSq = self.DEFAULT_MAX_CHI_SQ

        self.removeBackground = view.getRemoveBackground()

        # Validate that the user has write permissions as early as possible in the workflow.
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=self.runNumber, continueFlags=self.continueAnywayFlags
        )
        self.request(path="calibration/validateWritePermissions", payload=permissionsRequest)

        self._tweakPeakView.updateRunNumber(self.runNumber)
        self._saveView.updateRunNumber(self.runNumber)

        # fields with defaults
        self.convergenceThreshold = view.fieldConvergenceThreshold.get(self.DEFAULT_CONV)
        self.nBinsAcrossPeakWidth = view.fieldNBinsAcrossPeakWidth.get(self.DEFAULT_NBINS)

        self._tweakPeakView.populateGroupingDropdown(list(self.groupingMap.getMap(self.useLiteMode).keys()))
        self._tweakPeakView.updateFields(
            view.sampleDropdown.currentIndex(),
            view.groupingFileDropdown.currentIndex(),
            view.peakFunctionDropdown.currentIndex(),
        )
        self._tweakPeakView.updateMaxChiSq(self.maxChiSq)

        payload = self._createDiffCalRequest(
            xtalDMin=self.prevXtalDMin,
            xtalDMax=self.prevXtalDMax,
            peakFunction=self.peakFunction,
            fwhm=self.prevFWHM,
            maxChiSq=self.maxChiSq,
        )
        self.ingredients = self.request(path="calibration/ingredients", payload=payload).data
        self.groceries = self.request(path="calibration/groceries", payload=payload).data

        # set "previous" values -- this is their initialization
        # these are used to compare if the values have changed
        self.prevXtalDMin = payload.crystalDMin  # NOTE set in __init__ to defaults
        self.prevXtalDMax = payload.crystalDMax  # NOTE set in __init__ to defaults
        self.prevFWHM = payload.fwhmMultipliers  # NOTE set in __init__ to defaults
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()

        # TODO: These need to be moved to the workspace name generator
        self.fitPeaksDiagnostic = f"fit_peak_diag_{self.runNumber}_{self.prevGroupingIndex}_pre"

        self.residualWorkspace = f"diffcal_residual_{self.runNumber}"
        # focus the workspace to view the peaks
        self._renewPixelCal()
        self.groceries["inputWorkspace"] = self.pixelCalibratedWorkspace
        self._renewFocus(self.prevGroupingIndex)
        self._renewFitPeaks(self.peakFunction)
        response = self._calculateResidual()

        self._tweakPeakView.updateGraphs(
            self.focusedWorkspace,
            self.ingredients.groupedPeakLists,
            self.fitPeaksDiagnostic,
            self.residualWorkspace,
        )
        return response

    @ExceptionToErrLog
    @Slot(int, float, float, SymmetricPeakEnum, Pair, float)
    def onValueChange(self, *args):
        self._tweakPeakView.disableRecalculateButton()
        self.workflow.presenter.handleAction(
            self.renewWhenRecalculate,
            args=args,
            onSuccess=self._tweakPeakView.enableRecalculateButton,
        )

    def renewWhenRecalculate(self, groupingIndex, xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq):
        self._tweakPeakView.disableRecalculateButton()

        self.focusGroupPath = list(self.focusGroups.items())[groupingIndex][0]

        newSkipPixelSelection = self._tweakPeakView.skipPixelCalToggle.getState()

        # if the user made a change in skip pixelcal election, redo everything
        if self.skipPixelCal != newSkipPixelSelection:
            self.skipPixelCal = newSkipPixelSelection
            self._renewIngredients(xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq)
            self._renewPixelCal()
            self._renewFocus(groupingIndex)
            self._renewFitPeaks(peakFunction)
            self._calculateResidual()

        # if the grouping file changes, load new grouping and refocus
        elif groupingIndex != self.prevGroupingIndex:
            self._renewIngredients(xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq)
            self._renewFocus(groupingIndex)
            self._renewFitPeaks(peakFunction)
            self._calculateResidual()

        # if peaks will change, redo only the smoothing
        elif (
            xtalDMin != self.prevXtalDMin
            or xtalDMax != self.prevXtalDMax
            or peakFunction != self.peakFunction
            or fwhm != self.prevFWHM
            or maxChiSq != self.maxChiSq
            or self.peaksWerePurged
        ):
            self._renewIngredients(xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq)
            self._renewFitPeaks(peakFunction)
            self._calculateResidual()

        self.peaksWerePurged = False

        # NOTE it was determined pixel calibration NOT need to be re-calculated when peak params change.
        # However, if this requirement changes, the if at L282 should be combined with the if at 269,
        # and the order should be _renewIngredients --> _renewPixelCal --> _renewFocus --> _renewFitPeaks

        self._tweakPeakView.updateGraphs(
            self.focusedWorkspace,
            self.ingredients.groupedPeakLists,
            self.fitPeaksDiagnostic,
            self.residualWorkspace,
        )

        # update the values for next call to this method
        self.prevXtalDMin = xtalDMin
        self.prevXtalDMax = xtalDMax
        self.prevFWHM = fwhm
        self.peakFunction = peakFunction
        self.prevGroupingIndex = groupingIndex
        self.maxChiSq = maxChiSq

        return SNAPResponse(code=ResponseCode.OK)

    def _createDiffCalRequest(self, xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq) -> DiffractionCalibrationRequest:
        """
        Creates a standard diffraction calibration request in one location, so that the same parameters are always used.
        """
        return DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            # fiddly bits
            peakFunction=peakFunction,
            convergenceThreshold=self.convergenceThreshold,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            fwhmMultipliers=fwhm,
            crystalDMin=xtalDMin,
            crystalDMax=xtalDMax,
            maxChiSq=maxChiSq,
            removeBackground=self.removeBackground,
        )

    def _renewIngredients(self, xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq):
        payload = self._createDiffCalRequest(xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq)
        response = self.request(path="calibration/ingredients", payload=payload)
        self.ingredients = response.data
        return response

    def _renewPixelCal(self):
        if not self.skipPixelCal:
            payload = SimpleDiffCalRequest(
                ingredients=self.ingredients,
                groceries=self.groceries,
            )
            response = self.request(path="calibration/pixel", payload=payload).data
            self.prevDiffCal = response.calibrationTable
            self.pixelCalibratedWorkspace = response.outputWorkspace
        else:
            self.pixelCalibratedWorkspace = self.groceries["inputWorkspace"]
            self.prevDiffCal = self.groceries["previousCalibration"]

    def _renewFocus(self, groupingIndex):
        self.focusGroupPath = list(self.focusGroups.items())[groupingIndex][0]
        # send a request for the focused workspace
        payload = FocusSpectraRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            preserveEvents=False,
            inputWorkspace=self.groceries["inputWorkspace"],
            groupingWorkspace=self.groceries["groupingWorkspace"],
        )
        response = self.request(path="calibration/focus", payload=payload)
        self.focusedWorkspace = response.data[0]
        self.groceries["groupingWorkspace"] = response.data[1]
        return response

    def _renewFitPeaks(self, peakFunction):
        payload = FitMultiplePeaksRequest(
            inputWorkspace=self.focusedWorkspace,
            outputWorkspaceGroup=self.fitPeaksDiagnostic,
            detectorPeaks=self.ingredients.groupedPeakLists,
            peakFunction=peakFunction,
        )

        response = self.request(path="calibration/fitpeaks", payload=payload)
        return response

    def _calculateResidual(self):
        payload = CalculateResidualRequest(
            inputWorkspace=self.focusedWorkspace,
            outputWorkspace=self.residualWorkspace,
            fitPeaksDiagnosticWorkspace=self.fitPeaksDiagnostic,
        )
        return self.request(path="calibration/residual", payload=payload)

    @ExceptionToErrLog
    @Slot(float)
    def purgeBadPeaks(self, maxChiSq):
        self._tweakPeakView.disableRecalculateButton()
        self.workflow.presenter.handleAction(
            self._purgeBadPeaks,
            args=(maxChiSq,),
            onSuccess=self._tweakPeakView.enableRecalculateButton,
        )

    def _purgeBadPeaks(self, maxChiSq):
        # update the max chi sq
        self.maxChiSq = maxChiSq
        allPeaks = self.ingredients.groupedPeakLists
        param_table = self.mantidSnapper.mtd[self.fitPeaksDiagnostic].getItem(FitOutputEnum.Parameters.value).toDict()
        index = param_table["wsindex"]
        allChi2 = param_table["chi2"]
        goodPeaks = []
        for wkspIndex, groupPeaks in enumerate(allPeaks):
            peaks = groupPeaks.peaks
            # collect the fit chi-sq parameters for this spectrum, and the fits
            chi2 = [x2 for i, x2 in zip(index, allChi2) if i == wkspIndex]
            goodPeaks.append([peak for x2, peak in zip(chi2, peaks) if x2 < maxChiSq])
        too_fews = [goodPeak for goodPeak in goodPeaks if len(goodPeak) < 2]
        if too_fews != []:
            msg = """
            Too Few Peaks
            Purging would result in fewer than the required 2 peaks for calibration.
            The current set of peaks will be retained.
            """
            raise RuntimeError(msg)
        else:
            for wkspIndex, originalGroupPeaks in enumerate(allPeaks):
                originalGroupPeaks.peaks = goodPeaks[wkspIndex]
            self.ingredients.groupedPeakLists = allPeaks
            self.peaksWerePurged = True

        # renew the fits to the peaks
        self._renewFitPeaks(self.peakFunction)

        # update graph with reduced peak list
        self._tweakPeakView.updateGraphs(
            self.focusedWorkspace,
            self.ingredients.groupedPeakLists,
            self.fitPeaksDiagnostic,
            self.residualWorkspace,
        )
        return SNAPResponse(code=ResponseCode.OK)

    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _triggerDiffractionCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        self.runNumber = view.runNumberField.text()
        self._saveView.updateRunNumber(self.runNumber)
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.groceries["previousCalibration"] = self.prevDiffCal

        # Cleanup Excess Workspaces
        localKeeps = []
        for grocery in self.groceries.values():
            localKeeps.append(grocery)

        self._clearWorkspaces(exclude=set(localKeeps), clearCachedWorkspaces=False)

        # perform the group calibration
        payload = SimpleDiffCalRequest(
            ingredients=self.ingredients,
            groceries=self.groceries,
        )
        response = self.request(path="calibration/group", payload=payload).data

        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=self.runNumber),
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            workspaces={
                wngt.DIFFCAL_OUTPUT: [response.outputWorkspace],
                wngt.DIFFCAL_DIAG: [response.diagnosticWorkspace],
                wngt.DIFFCAL_TABLE: [response.calibrationTable],
                wngt.DIFFCAL_MASK: [response.maskWorkspace],
            },
            # fiddly bits
            peakFunction=self.peakFunction,
            crystalDMin=self.prevXtalDMin,
            crystalDMax=self.prevXtalDMax,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            fwhmMultipliers=self.prevFWHM,
            maxChiSq=self.maxChiSq,
        )

        response = self.request(path="calibration/assessment", payload=payload)
        self.assessmentResponse = response.data

        if "afterCrossCor" in self.pixelCalibratedWorkspace:
            # rename self.pixelCalibratedWorkspace
            renameRequest = RenameWorkspaceRequest(
                oldName=self.pixelCalibratedWorkspace, newName=f"__{self.pixelCalibratedWorkspace}"
            )
            self.request(path="workspace/rename", payload=renameRequest)

        self.outputs.update(self.assessmentResponse.metricWorkspaces)
        for calibrationWorkspaces in self.assessmentResponse.workspaces.values():
            self.outputs.update(calibrationWorkspaces)
        self._assessmentView.updateRunNumber(self.runNumber, self.useLiteMode)
        return response

    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _assessCalibration(self, workflowPresenter):  # noqa: ARG002
        if workflowPresenter.iteration > 1:
            self._saveView.enableIterationDropdown()
            iterations = [str(i) for i in range(0, workflowPresenter.iteration)]
            self._saveView.setIterationDropdown(iterations)
        return self.responses[-1]  # [-1]: response from CalibrationAssessmentRequest for the calibration in progress

    def _getSaveSelection(self, dropDown):
        selection = dropDown.currentText()
        if selection == self._saveView.currentIterationText:
            return self.assessmentResponse.workspaces

        iteration = int(selection)
        return {
            wsKey: [self.renameTemplate.format(workspaceName=wsName, iteration=iteration) for wsName in wsNames]
            for wsKey, wsNames in self.assessmentResponse.workspaces.items()
        }

    def _resetSaveView(self):
        self._saveView.hideIterationDropdown()
        self._saveView.resetIterationDropdown()

    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _saveCalibration(self, workflowPresenter):
        view: DiffCalSaveView = workflowPresenter.widget.tabView
        runNumber, version, appliesTo, comments, author = view.validateAndReadForm()

        # if this is not the first iteration, account for choice.
        if workflowPresenter.iteration > 1:
            self.assessmentResponse.workspaces = self._getSaveSelection(self._saveView.iterationDropdown)

        createIndexEntryRequest = CreateIndexEntryRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            appliesTo=appliesTo,
            comments=comments,
            author=author,
        )
        createRecordRequest = CreateCalibrationRecordRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            calculationParameters=self.assessmentResponse.calculationParameters,
            crystalInfo=self.assessmentResponse.crystalInfo,
            pixelGroups=self.assessmentResponse.pixelGroups,
            focusGroupCalibrationMetrics=self.assessmentResponse.focusGroupCalibrationMetrics,
            workspaces=self.assessmentResponse.workspaces,
            indexEntry=createIndexEntryRequest,
        )
        payload = CalibrationExportRequest(
            createRecordRequest=createRecordRequest,
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

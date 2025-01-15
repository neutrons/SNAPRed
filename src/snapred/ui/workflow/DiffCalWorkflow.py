from mantid.simpleapi import mtd
from qtpy.QtCore import Slot

from snapred.backend.dao import RunConfig
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VersionedObject, VersionState
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
    SimpleDiffCalRequest,
)
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.meta.Config import Config
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

    DEFAULT_DMIN = Config["constants.CrystallographicInfo.crystalDMin"]
    DEFAULT_DMAX = Config["constants.CrystallographicInfo.crystalDMax"]
    DEFAULT_NBINS = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    DEFAULT_CONV = Config["calibration.diffraction.convergenceThreshold"]
    DEFAULT_MAX_CHI_SQ = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    def __init__(self, parent=None):
        super().__init__(parent)
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
        self._requestView.litemodeToggle.stateChanged.connect(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._tweakPeakView.signalValueChanged.connect(self.onValueChange)
        self._tweakPeakView.signalPurgeBadPeaks.connect(self.purgeBadPeaks)

        # connect the lite mode toggles across the views
        self._requestView.litemodeToggle.stateChanged.connect(self._tweakPeakView.litemodeToggle.setState)
        self._tweakPeakView.litemodeToggle.stateChanged.connect(self._requestView.litemodeToggle.setState)

        # connect the skip pixelcal toggles across the views
        self._requestView.skipPixelCalToggle.stateChanged.connect(self._tweakPeakView.skipPixelCalToggle.setState)
        self._tweakPeakView.skipPixelCalToggle.stateChanged.connect(self._requestView.skipPixelCalToggle.setState)

        self.prevFWHM = DiffCalTweakPeakView.FWHM
        self.prevXtalDMin = DiffCalTweakPeakView.XTAL_DMIN
        self.prevXtalDMax = DiffCalTweakPeakView.XTAL_DMAX

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

    def _continueAnywayHandlerTweak(self, continueInfo: ContinueWarning.Model):  # noqa: ARG002
        self._continueAnywayHandler(continueInfo)
        self._tweakPeakView.updateContinueAnyway(True)

    def __setInteraction(self, state: bool):
        self._requestView.litemodeToggle.setEnabled(state)
        self._requestView.skipPixelCalToggle.setEnabled(state)
        self._requestView.groupingFileDropdown.setEnabled(state)

    @ExceptionToErrLog
    @Slot()
    def _populateGroupingDropdown(self):
        # when the run number is updated, freeze the drop down to populate it
        runNumber = self._requestView.runNumberField.text()
        useLiteMode = self._requestView.litemodeToggle.getState()

        self.__setInteraction(False)
        self.workflow.presenter.handleAction(
            self.handleDropdown,
            args=(runNumber, useLiteMode),
            onSuccess=lambda: self.__setInteraction(True),
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

    @ExceptionToErrLog
    @Slot()
    def _switchLiteNativeGroups(self):
        # determine resolution mode
        useLiteMode = self._requestView.litemodeToggle.getState()

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
        self.useLiteMode = view.litemodeToggle.getState()
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
        else:
            self.prevDiffCal = self.groceries["previousCalibration"]

    def _renewFocus(self, groupingIndex):
        self.focusGroupPath = list(self.focusGroups.items())[groupingIndex][0]
        # send a request for the focused workspace
        payload = FocusSpectraRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
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
            fitPeaksDiagnostic=self.fitPeaksDiagnostic,
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
        param_table = mtd[self.fitPeaksDiagnostic].getItem(FitOutputEnum.Parameters.value).toDict()
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
        assessmentResponse = response.data
        self.calibrationRecord = assessmentResponse.record

        self.outputs.extend(assessmentResponse.metricWorkspaces)
        for calibrationWorkspaces in self.calibrationRecord.workspaces.values():
            self.outputs.extend(calibrationWorkspaces)
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
            return self.calibrationRecord.workspaces

        iteration = int(selection)
        return {
            wsKey: [self.renameTemplate.format(workspaceName=wsName, iteration=iteration) for wsName in wsNames]
            for wsKey, wsNames in self.calibrationRecord.workspaces.items()
        }

    def _resetSaveView(self):
        self._saveView.hideIterationDropdown()
        self._saveView.resetIterationDropdown()

    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        runNumber = view.fieldRunNumber.get()
        version = view.fieldVersion.get(VersionState.NEXT)
        appliesTo = view.fieldAppliesTo.get(f">={runNumber}")
        # validate the version number
        version = VersionedObject(version=version).version
        # validate appliesTo field
        appliesTo = IndexEntry.appliesToFormatChecker(appliesTo)

        # if this is not the first iteration, account for choice.
        if workflowPresenter.iteration > 1:
            self.calibrationRecord.workspaces = self._getSaveSelection(self._saveView.iterationDropdown)

        createIndexEntryRequest = CreateIndexEntryRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            appliesTo=appliesTo,
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
        )
        createRecordRequest = CreateCalibrationRecordRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            calculationParameters=self.calibrationRecord.calculationParameters,
            crystalInfo=self.calibrationRecord.crystalInfo,
            pixelGroups=self.calibrationRecord.pixelGroups,
            focusGroupCalibrationMetrics=self.calibrationRecord.focusGroupCalibrationMetrics,
            workspaces=self.calibrationRecord.workspaces,
        )
        payload = CalibrationExportRequest(
            createIndexEntryRequest=createIndexEntryRequest,
            createRecordRequest=createRecordRequest,
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

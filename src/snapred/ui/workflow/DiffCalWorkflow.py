from snapred.backend.dao import RunConfig
from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
    FitMultiplePeaksRequest,
    FocusSpectraRequest,
    HasStateRequest,
)
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
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
    DEFAULT_PEAK_THRESHOLD = Config["calibration.diffraction.peakIntensityThreshold"]
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
        self._requestView.litemodeToggle.field.connectUpdate(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._tweakPeakView.signalValueChanged.connect(self.onValueChange)

        self.prevFWHM = DiffCalTweakPeakView.FWHM

        # 1. input run number and other basic parameters
        # 2. display peak graphs, allow adjustments to view
        # 3. perform diffraction calibration after user approves peaks then move on
        # 4. user assesses calibration and chooses to iterate, or continue
        # 5. user may elect to save the calibration
        self.workflow = (
            WorkflowBuilder(cancelLambda=self.resetWithPermission, iterateLambda=self._iterate, parent=parent)
            .addNode(self._specifyRun, self._requestView, "Diffraction Calibration")
            .addNode(self._triggerDiffractionCalibration, self._tweakPeakView, "Tweak Peak Peek")
            .addNode(self._assessCalibration, self._assessmentView, "Assessing", iterate=True)
            .addNode(self._saveCalibration, self._saveView, name="Saving")
            .build()
        )
        self.workflow.presenter.setResetLambda(self.reset)

    @ExceptionToErrLog
    def _populateGroupingDropdown(self):
        # when the run number is updated, freeze the drop down to populate it
        runNumber = self._requestView.runNumberField.text()
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        self._requestView.litemodeToggle.setEnabled(False)
        # TODO: Use threads, account for fail cases
        try:
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

            # populate and reenable the drop down
            self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        except Exception as e:  # noqa BLE001
            print(e)

        self._requestView.groupingFileDropdown.setEnabled(True)
        self._requestView.litemodeToggle.setEnabled(True)

    @ExceptionToErrLog
    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        # TODO: Use threads, account for fail cases
        try:
            self.focusGroups = self.groupingMap.getMap(useLiteMode)
            self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        except Exception as e:  # noqa BLE001
            print(e)

        self._requestView.groupingFileDropdown.setEnabled(True)

    def _specifyRun(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        # fetch the data from the view
        self.runNumber = view.runNumberField.text()
        self.useLiteMode = view.litemodeToggle.field.getState()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.calibrantSamplePath = view.sampleDropdown.currentText()
        self.peakFunction = view.peakFunctionDropdown.currentText()
        self.maxChiSq = self.DEFAULT_MAX_CHI_SQ

        self._tweakPeakView.updateRunNumber(self.runNumber)
        self._saveView.updateRunNumber(self.runNumber)

        # fields with defaults
        self.convergenceThreshold = view.fieldConvergenceThreshold.get(self.DEFAULT_CONV)
        self.nBinsAcrossPeakWidth = view.fieldNBinsAcrossPeakWidth.get(self.DEFAULT_NBINS)
        self.peakThreshold = view.fieldPeakIntensityThreshold.get(self.DEFAULT_PEAK_THRESHOLD)

        self._tweakPeakView.populateGroupingDropdown(list(self.groupingMap.getMap(self.useLiteMode).keys()))
        self._tweakPeakView.updateFields(
            view.sampleDropdown.currentIndex(),
            view.groupingFileDropdown.currentIndex(),
            view.peakFunctionDropdown.currentIndex(),
        )
        self._tweakPeakView.updatePeakThreshold(self.peakThreshold)
        self._tweakPeakView.updateMaxChiSq(self.maxChiSq)

        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            # fiddly bits
            peakFunction=self.peakFunction,
            peakIntensityThreshold=self.peakThreshold,
            convergenceThreshold=self.convergenceThreshold,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            fwhmMultipliers=self.prevFWHM,
            maxChiSq=self.maxChiSq,
        )

        self.ingredients = self.request(path="calibration/ingredients", payload=payload.json()).data
        self.groceries = self.request(path="calibration/groceries", payload=payload.json()).data

        # set "previous" values -- this is their initialization
        # these are used to compare if the values have changed
        self.prevXtalDMin = payload.crystalDMin
        self.prevXtalDMax = payload.crystalDMax
        self.prevThreshold = payload.peakIntensityThreshold
        self.prevFWHM = payload.fwhmMultipliers  # NOTE set in __init__ to defaults
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()
        self.fitPeaksDiagnostic = f"fit_peak_diag_{self.runNumber}_{self.prevGroupingIndex}_pre"

        # focus the workspace to view the peaks
        self._renewFocus(self.prevGroupingIndex)
        response = self._renewFitPeaks(self.peakFunction)

        self._tweakPeakView.updateGraphs(
            self.focusedWorkspace,
            self.ingredients.groupedPeakLists,
            self.fitPeaksDiagnostic,
        )
        return response

    @ExceptionToErrLog
    def onValueChange(self, groupingIndex, xtalDMin, xtalDMax, peakThreshold, peakFunction, fwhm, maxChiSq):
        self._tweakPeakView.disableRecalculateButton()
        # TODO: This is a temporary solution,
        # this should have never been setup to all run on the same thread.
        # It assumed an exception would never be tossed and thus
        # would never enable the recalc button again if one did
        try:
            self.focusGroupPath = list(self.focusGroups.items())[groupingIndex][0]

            # if peaks will change, redo only the smoothing
            xtalDMinValueChanged = xtalDMin != self.prevXtalDMin
            xtalDMaxValueChanged = xtalDMax != self.prevXtalDMax
            thresholdChanged = peakThreshold != self.prevThreshold
            peakFunctionChanged = peakFunction != self.peakFunction
            fwhmChanged = fwhm != self.prevFWHM
            maxChiSqChanged = maxChiSq != self.maxChiSq
            if (
                xtalDMinValueChanged
                or xtalDMaxValueChanged
                or thresholdChanged
                or peakFunctionChanged
                or fwhmChanged
                or maxChiSqChanged
            ):
                self._renewIngredients(xtalDMin, xtalDMax, peakThreshold, peakFunction, fwhm, maxChiSq)
                self._renewFitPeaks(peakFunction)

            # if the grouping file changes, load new grouping and refocus
            if groupingIndex != self.prevGroupingIndex:
                self._renewIngredients(xtalDMin, xtalDMax, peakThreshold, peakFunction, fwhm, maxChiSq)
                self._renewFocus(groupingIndex)
                self._renewFitPeaks(peakFunction)

            self._tweakPeakView.updateGraphs(
                self.focusedWorkspace,
                self.ingredients.groupedPeakLists,
                self.fitPeaksDiagnostic,
            )

            # update the values for next call to this method
            self.prevXtalDMin = xtalDMin
            self.prevXtalDMax = xtalDMax
            self.prevFWHM = fwhm
            self.prevThreshold = peakThreshold
            self.peakFunction = peakFunction
            self.prevGroupingIndex = groupingIndex
            self.maxChiSq = maxChiSq
        except Exception as e:  # noqa BLE001
            print(e)

        # renable button when graph is updated
        self._tweakPeakView.enableRecalculateButton()

    def _renewIngredients(self, xtalDMin, xtalDMax, peakThreshold, peakFunction, fwhm, maxChiSq):
        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            # fiddly bits
            peakFunction=peakFunction,
            crystalDMin=xtalDMin,
            crystalDMax=xtalDMax,
            peakIntensityThreshold=peakThreshold,
            fwhmMultipliers=fwhm,
            maxChiSq=maxChiSq,
        )
        response = self.request(path="calibration/ingredients", payload=payload.json())
        self.ingredients = response.data
        return response

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
        response = self.request(path="calibration/focus", payload=payload.json())
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
        return self.request(path="calibration/fitpeaks", payload=payload.json())

    def _triggerDiffractionCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        self.runNumber = view.runNumberField.text()
        self._saveView.updateRunNumber(self.runNumber)
        self.focusGroupPath = view.groupingFileDropdown.currentText()

        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            calibrantSamplePath=self.calibrantSamplePath,
            focusGroup=self.focusGroups[self.focusGroupPath],
            useLiteMode=self.useLiteMode,
            # fiddly bits
            peakFunction=self.peakFunction,
            crystalDMin=self.prevXtalDMin,
            crystalDMax=self.prevXtalDMax,
            peakIntensityThreshold=self.prevThreshold,
            convergenceThreshold=self.convergenceThreshold,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            fwhmMultipliers=self.prevFWHM,
            maxChiSq=self.maxChiSq,
        )

        response = self.request(path="calibration/diffraction", payload=payload.json())

        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=self.runNumber),
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            workspaces={
                wngt.DIFFCAL_OUTPUT: response.data["outputWorkspace"],
                wngt.DIFFCAL_DIAG: response.data["diagnosticWorkspace"],
                wngt.DIFFCAL_TABLE: response.data["calibrationTable"],
                wngt.DIFFCAL_MASK: response.data["maskWorkspace"],
            },
            # fiddly bits
            peakFunction=self.peakFunction,
            crystalDMin=self.prevXtalDMin,
            crystalDMax=self.prevXtalDMax,
            peakIntensityThreshold=self.prevThreshold,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            fwhmMultipliers=self.prevFWHM,
            maxChiSq=self.maxChiSq,
        )

        response = self.request(path="calibration/assessment", payload=payload.json())
        assessmentResponse = response.data
        self.calibrationRecord = assessmentResponse.record

        self.outputs.extend(assessmentResponse.metricWorkspaces)
        for calibrationWorkspaces in self.calibrationRecord.workspaces.values():
            self.outputs.append(calibrationWorkspaces)
        self._assessmentView.updateRunNumber(self.runNumber, self.useLiteMode)
        return response

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
            wsKey: self.renameTemplate.format(workspaceName=wsName, iteration=iteration)
            for wsKey, wsName in self.calibrationRecord.workspaces.items()
        }

    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        version = view.fieldVersion.get(None)
        # validate the version number
        version = view.fieldVersion.get(None)
        if version is not None:
            try:
                version = int(version)
                assert version >= 0
            except (AssertionError, ValueError, TypeError):
                raise TypeError("Version must be a nonnegative integer")
        # pull fields from view for calibration save
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            useLiteMode=self.useLiteMode,
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
            appliesTo=view.fieldAppliesTo.get(),
        )

        # if this is not the first iteration, account for choice.
        if workflowPresenter.iteration > 1:
            self.calibrationRecord.workspaces = self._getSaveSelection(self._saveView.iterationDropdown)

        payload = CalibrationExportRequest(
            version=version,
            calibrationRecord=self.calibrationRecord,
            calibrationIndexEntry=calibrationIndexEntry,
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

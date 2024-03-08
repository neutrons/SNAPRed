import json

from snapred.backend.dao import RunConfig
from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
    FocusSpectraRequest,
)
from snapred.backend.dao.response.CalibrationAssessmentResponse import CalibrationAssessmentResponse
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView
from snapred.ui.view.DiffCalRequestView import DiffCalRequestView
from snapred.ui.view.DiffCalSaveView import DiffCalSaveView
from snapred.ui.view.DiffCalTweakPeakView import DiffCalTweakPeakView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class DiffCalWorkflow(WorkflowImplementer):
    DEFAULT_DMIN = Config["constants.CrystallographicInfo.dMin"]
    DEFAULT_DMAX = Config["constants.CrystallographicInfo.dMin"]
    DEFAULT_NBINS = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    DEFAULT_CONV = Config["calibration.diffraction.convergenceThreshold"]
    DEFAULT_PEAK_THRESHOLD = Config["calibration.diffraction.peakIntensityThreshold"]

    def __init__(self, jsonForm, parent=None):
        super().__init__(parent)
        # create a tree of flows for the user to successfully execute diffraction calibration
        # DiffCal Request ->
        # Check Peaks     ->
        # Calibrate       ->
        # Assess          ->
        # Save?           ->

        self.assessmentSchema = self.request(path="api/parameters", payload="calibration/assessment").data
        # for each key, read string and convert to json
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        self.saveSchema = self.request(path="api/parameters", payload="calibration/save").data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}

        self.samplePaths = self.request(path="config/samplePaths").data
        self.defaultGroupingMap = self.request(path="config/groupingMap", payload="tmfinr").data
        self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.lite

        self._requestView = DiffCalRequestView(
            jsonForm,
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._tweakPeakView = DiffCalTweakPeakView(
            jsonForm,
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._assessmentView = DiffCalAssessmentView(
            "Assessing Calibration",
            self.assessmentSchema,
            parent=parent,
        )
        self._saveView = DiffCalSaveView(parent)

        # connect signal to populate the grouping dropdown after run is selected
        self._requestView.litemodeToggle.field.connectUpdate(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._tweakPeakView.signalValueChanged.connect(self.onValueChange)

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

    def _populateGroupingDropdown(self):
        # when the run number is updated, freeze the drop down to populate it
        runNumber = self._requestView.runNumberField.text()
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        self._requestView.litemodeToggle.setEnabled(False)

        # check if the state exists -- if so load its grouping map
        hasState = self.request(path="calibration/hasState", payload=runNumber).data
        if hasState:
            self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
        else:
            self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(useLiteMode)

        # populate and reenable the drop down
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._requestView.groupingFileDropdown.setEnabled(True)
        self._requestView.litemodeToggle.setEnabled(True)

    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        self.focusGroups = self.groupingMap.getMap(useLiteMode)
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._requestView.groupingFileDropdown.setEnabled(True)

    def _specifyRun(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view
        self.verifyForm(view)

        # fetch the data from the view
        self.runNumber = view.runNumberField.text()
        self.useLiteMode = view.litemodeToggle.field.getState()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.calibrantSamplePath = view.sampleDropdown.currentText()
        self.peakFunction = view.peakFunctionDropdown.currentText()

        self._tweakPeakView.updateRunNumber(self.runNumber)
        self._saveView.updateRunNumber(self.runNumber)

        # fields with defaults
        self.convergenceThreshold = view.fieldConvergenceThreshold.get(self.DEFAULT_CONV)
        self.nBinsAcrossPeakWidth = view.fieldNBinsAcrossPeakWidth.get(self.DEFAULT_NBINS)
        self.peakThreshold = view.fieldPeakIntensityThreshold.get(self.DEFAULT_PEAK_THRESHOLD)

        self._tweakPeakView.populateGroupingDropdown(list(self.groupingMap.getMap(self.useLiteMode).keys()))
        self._tweakPeakView.updateFields(
            self.runNumber,
            view.sampleDropdown.currentIndex(),
            view.groupingFileDropdown.currentIndex(),
            view.peakFunctionDropdown.currentIndex(),
        )

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
        )

        self.ingredients = self.request(path="calibration/ingredients", payload=payload.json()).data
        self.groceries = self.request(path="calibration/groceries", payload=payload.json()).data

        # set "previous" values -- this is their initialization
        # these are used to compare if the values have changed
        self.prevDMin = payload.crystalDMin
        self.prevDMax = payload.crystalDMax
        self.prevThreshold = payload.peakIntensityThreshold
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()

        # focus the workspace to view the peaks
        payload = FocusSpectraRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            inputWorkspace=self.groceries["inputWorkspace"],
            groupingWorkspace=self.groceries["groupingWorkspace"],
        )
        response = self.request(path="calibration/focus", payload=payload.json())
        self.focusedWorkspace = response.data[0]
        self._tweakPeakView.updateGraphs(self.focusedWorkspace, self.ingredients.groupedPeakLists)
        return response

    def onValueChange(self, groupingIndex, dMin, dMax, peakThreshold):
        if groupingIndex < 0:
            raise RuntimeError("YOU IDIOT")
        self._tweakPeakView.disableRecalculateButton()

        self.focusGroupPath = list(self.focusGroups.items())[groupingIndex][0]

        # if peaks will change, redo only the smoothing
        dMinValueChanged = dMin != self.prevDMin
        dMaxValueChanged = dMax != self.prevDMax
        thresholdChanged = peakThreshold != self.prevThreshold
        if dMinValueChanged or dMaxValueChanged or thresholdChanged:
            self._renewIngredients(dMin, dMax, peakThreshold)

        # if the grouping file changes, load new grouping and refocus
        if groupingIndex != self.prevGroupingIndex:
            self._renewFocus(groupingIndex)
            self._renewIngredients(dMin, dMax, peakThreshold)

        self._tweakPeakView.updateGraphs(self.focusedWorkspace, self.ingredients.groupedPeakLists)

        # renable button when graph is updated
        self._tweakPeakView.enableRecalculateButton()

        # update the values for next call to this method
        self.prevDMin = dMin
        self.prevDMax = dMax
        self.prevThreshold = peakThreshold
        self.prevGroupingIndex = groupingIndex

    def _renewIngredients(self, dMin, dMax, peakThreshold):
        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            focusGroup=self.focusGroups[self.focusGroupPath],
            calibrantSamplePath=self.calibrantSamplePath,
            # fiddly bits
            peakFunction=self.peakFunction,
            crystalDMin=dMin,
            crystalDMax=dMax,
            peakIntensityThreshold=peakThreshold,
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

    def _triggerDiffractionCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for diffraction calibration
        self.verifyForm(view)

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
            crystalDMin=self.prevDMin,
            crystalDMax=self.prevDMax,
            peakIntensityThreshold=self.prevThreshold,
            convergenceThreshold=self.convergenceThreshold,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
        )

        response = self.request(path="calibration/diffraction", payload=payload.json())

        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=self.runNumber),
            workspaces={
                wngt.DIFFCAL_OUTPUT: [response.data["outputWorkspace"]],
                wngt.DIFFCAL_TABLE: [response.data["calibrationTable"]],
                wngt.DIFFCAL_MASK: [response.data["maskWorkspace"]],
            },
            focusGroup=self.focusGroups[self.focusGroupPath],
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            useLiteMode=self.useLiteMode,
            calibrantSamplePath=self.calibrantSamplePath,
        )

        response = self.request(path="calibration/assessment", payload=payload.json())
        assessmentResponse = response.data
        self.calibrationRecord = assessmentResponse.record

        self.outputs.extend(assessmentResponse.metricWorkspaces)
        for calibrationWorkspaces in self.calibrationRecord.workspaces.values():
            self.outputs.extend(calibrationWorkspaces)
        self._assessmentView.updateRunNumber(self.runNumber)
        return response

    def _assessCalibration(self, workflowPresenter):  # noqa: ARG002
        if workflowPresenter.iteration > 1:
            self._saveView.enableIterationDropdown()
            iterations = [str(i) for i in range(0, workflowPresenter.iteration)]
            self._saveView.iterationDropdown.clear()
            self._saveView.iterationDropdown.addItems(iterations)
            self._saveView.iterationDropdown.setCurrentIndex(workflowPresenter.iteration - 1)
        return self.responses[-1]  # [-1]: response from CalibrationAssessmentRequest for the calibration in progress

    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration save
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
            appliesTo=view.fieldAppliesTo.get(),
        )

        # if this is not the first iteration, account for choice.
        if workflowPresenter.iteration > 1:
            iteration = int(self._saveView.iterationDropdown.currentText())
            self.calibrationRecord.workspaces = {
                wsKey: [self.renameTemplate.format(workspaceName=wsName, iteration=iteration) for wsName in wsNames]
                for wsKey, wsNames in self.calibrationRecord.workspaces.items()
            }

        payload = CalibrationExportRequest(
            calibrationRecord=self.calibrationRecord, calibrationIndexEntry=calibrationIndexEntry
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

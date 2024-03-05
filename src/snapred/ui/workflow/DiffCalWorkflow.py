import json

from snapred.backend.dao import RunConfig
from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
)
from snapred.backend.dao.response.CalibrationAssessmentResponse import CalibrationAssessmentResponse
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.DiffCalAssessmentView import DiffCalAssessmentView
from snapred.ui.view.DiffCalRequestView import DiffCalRequestView
from snapred.ui.view.DiffCalSaveView import DiffCalSaveView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class DiffCalWorkflow(WorkflowImplementer):
    def __init__(self, jsonForm, parent=None):
        super().__init__(parent)
        # create a tree of flows for the user to successfully execute diffraction calibration
        # Calibrate     ->
        # Assess        ->
        # Save?         ->

        self.assessmentSchema = self.request(path="api/parameters", payload="calibration/assessment").data
        # for each key, read string and convert to json
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        self.saveSchema = self.request(path="api/parameters", payload="calibration/save").data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}

        self.samplePaths = self.request(path="config/samplePaths").data
        self.defaultGroupingMap = self.request(path="config/groupingMap", payload="tmfinr").data
        self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.lite

        self._calibrationReductionView = DiffCalRequestView(
            jsonForm, samples=self.samplePaths, groups=list(self.focusGroups.keys()), parent=parent
        )
        self._calibrationAssessmentView = DiffCalAssessmentView(
            "Assessing Calibration", self.assessmentSchema, parent=parent
        )
        self._saveCalibrationView = DiffCalSaveView(parent)

        # connect signal to populate the grouping dropdown after run is selected
        self._calibrationReductionView.litemodeToggle.field.connectUpdate(self._switchLiteNativeGroups)
        self._calibrationReductionView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)

        self.workflow = (
            WorkflowBuilder(cancelLambda=self.resetWithPermission, iterateLambda=self._iterate, parent=parent)
            .addNode(
                self._triggerCalibrationReduction,
                self._calibrationReductionView,
                "Calibrating",
            )
            .addNode(self._assessCalibration, self._calibrationAssessmentView, "Assessing", iterate=True)
            .addNode(self._saveCalibration, self._saveCalibrationView, name="Saving")
            .build()
        )

    def _populateGroupingDropdown(self):
        # when the run number is updated, freeze the drop down to populate it
        runNumber = self._calibrationReductionView.runNumberField.text()
        useLiteMode = self._calibrationReductionView.litemodeToggle.field.getState()

        self._calibrationReductionView.groupingFileDropdown.setEnabled(False)
        self._calibrationReductionView.litemodeToggle.setEnabled(False)

        # check if the state exists -- if so load its grouping map
        hasState = self.request(path="calibration/hasState", payload=runNumber).data
        if hasState:
            self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
        else:
            self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(useLiteMode)

        # populate and reenable the drop down
        self._calibrationReductionView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._calibrationReductionView.groupingFileDropdown.setEnabled(True)
        self._calibrationReductionView.litemodeToggle.setEnabled(True)

    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._calibrationReductionView.litemodeToggle.field.getState()

        self._calibrationReductionView.groupingFileDropdown.setEnabled(False)
        self.focusGroups = self.groupingMap.getMap(useLiteMode)
        self._calibrationReductionView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._calibrationReductionView.groupingFileDropdown.setEnabled(True)

    def _triggerCalibrationReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration reduction
        self.verifyForm(view)

        self.runNumber = view.runNumberField.text()

        self._saveCalibrationView.updateRunNumber(self.runNumber)

        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.useLiteMode = view.litemodeToggle.field.getState()
        self.calibrantSamplePath = view.sampleDropdown.currentText()
        self.peakFunction = view.peakFunctionDropdown.currentText()

        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            calibrantSamplePath=self.calibrantSamplePath,
            focusGroup=self.focusGroups[self.focusGroupPath],
            useLiteMode=self.useLiteMode,
            peakFunction=self.peakFunction,
        )
        payload.convergenceThreshold = view.fieldConvergnceThreshold.get(payload.convergenceThreshold)
        payload.peakIntensityThreshold = view.fieldPeakIntensityThreshold.get(payload.peakIntensityThreshold)
        payload.nBinsAcrossPeakWidth = view.fieldNBinsAcrossPeakWidth.get(payload.nBinsAcrossPeakWidth)
        self.nBinsAcrossPeakWidth = payload.nBinsAcrossPeakWidth

        response = self.request(path="calibration/diffraction", payload=payload.json())

        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=self.runNumber),
            workspace=self.responses[-1].data["outputWorkspace"],
            focusGroup=self.focusGroups[self.focusGroupPath],
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            useLiteMode=self.useLiteMode,
            calibrantSamplePath=self.calibrantSamplePath,
        )

        response = self.request(path="calibration/assessment", payload=payload.json())
        assessmentResponse = response.data
        self.calibrationRecord = assessmentResponse.record
        self.calibrationRecord.workspaceNames.append(self.responses[-2].data["calibrationTable"])
        self.calibrationRecord.workspaceNames.append(self.responses[-2].data["maskWorkspace"])

        self.outputs.extend(assessmentResponse.metricWorkspaces)
        self.outputs.extend(self.calibrationRecord.workspaceNames)
        return response

    def _assessCalibration(self, workflowPresenter):  # noqa: ARG002
        if workflowPresenter.iteration > 1:
            self._saveCalibrationView.enableIterationDropdown()
            iterations = [str(i) for i in range(0, workflowPresenter.iteration)]
            self._saveCalibrationView.iterationDropdown.clear()
            self._saveCalibrationView.iterationDropdown.addItems(iterations)
            self._saveCalibrationView.iterationDropdown.setCurrentIndex(workflowPresenter.iteration - 1)
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
            iteration = int(self._saveCalibrationView.iterationDropdown.currentText())
            self.calibrationRecord.workspaceNames = [
                self.renameTemplate.format(workspaceName=w, iteration=iteration)
                for w in self.calibrationRecord.workspaceNames
            ]

        payload = CalibrationExportRequest(
            calibrationRecord=self.calibrationRecord, calibrationIndexEntry=calibrationIndexEntry
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

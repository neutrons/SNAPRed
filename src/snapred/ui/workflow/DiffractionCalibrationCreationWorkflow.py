import json

from snapred.backend.dao import RunConfig
from snapred.backend.dao.calibration import CalibrationIndexEntry
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
)
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.CalibrationAssessmentView import CalibrationAssessmentView
from snapred.ui.view.CalibrationReductionRequestView import CalibrationReductionRequestView
from snapred.ui.view.SaveCalibrationView import SaveCalibrationView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class DiffractionCalibrationCreationWorkflow(WorkflowImplementer):
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

        self.focusGroups = self.request(path="config/focusGroups").data
        self.groupingFiles = list(self.focusGroups.keys())

        self._calibrationReductionView = CalibrationReductionRequestView(
            jsonForm, samples=self.samplePaths, groups=self.groupingFiles, parent=parent
        )
        self._calibrationAssessmentView = CalibrationAssessmentView(
            "Assessing Calibration", self.assessmentSchema, parent=parent
        )
        self._saveCalibrationView = SaveCalibrationView(parent)

        self.workflow = (
            WorkflowBuilder(cancelLambda=None, parent=parent)
            .addNode(
                self._triggerCalibrationReduction,
                self._calibrationReductionView,
                "Calibrating",
            )
            .addNode(
                self._assessCalibration,
                self._calibrationAssessmentView,
                "Assessing",
            )
            .addNode(self._saveCalibration, self._saveCalibrationView, name="Saving", required=False)
            .addNode(*self.iterateStepTuple)
            .build()
        )

    def _triggerCalibrationReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration reduction
        self.verifyForm(view)

        self.runNumber = view.runNumberField.text()

        self._saveCalibrationView.updateRunNumber(self.runNumber)

        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.useLiteMode = view.litemodeToggle.field.getState()
        self.calibrantSamplePath = view.sampleDropdown.currentText()

        payload = DiffractionCalibrationRequest(
            runNumber=self.runNumber,
            calibrantSamplePath=self.calibrantSamplePath,
            focusGroup=self.focusGroups[self.focusGroupPath],
            useLiteMode=self.useLiteMode,
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
        self.calibrationRecord = self.responses[-1].data
        self.calibrationRecord.workspaceNames.append(self.responses[-2].data["calibrationTable"])
        self.outputs.extend(self.calibrationRecord.workspaceNames)
        return response

    def _assessCalibration(self, workflowPresenter):  # noqa: ARG002
        return self.responses[-1]  # [-1]: response from CalibrationAssessmentRequest for the calibration in progress

    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration save
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
        )
        payload = CalibrationExportRequest(
            calibrationRecord=self.calibrationRecord, calibrationIndexEntry=calibrationIndexEntry
        )

        response = self.request(path="calibration/save", payload=payload.json())
        return response

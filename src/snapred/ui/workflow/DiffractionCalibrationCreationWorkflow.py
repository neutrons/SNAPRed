import json

from qtpy.QtWidgets import QLabel

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.dao.calibration import CalibrationIndexEntry, CalibrationRecord
from snapred.backend.dao.request import CalibrationAssessmentRequest, CalibrationExportRequest
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.CalibrationAssessmentView import CalibrationAssessmentView
from snapred.ui.view.CalibrationReductionRequestView import CalibrationReductionRequestView
from snapred.ui.view.SaveCalibrationView import SaveCalibrationView
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

logger = snapredLogger.getLogger(__name__)


class DiffractionCalibrationCreationWorkflow:
    def __init__(self, jsonForm, parent=None):
        # create a tree of flows for the user to successfully execute diffraction calibration
        # Calibrate     ->
        # Assess        ->
        # Save?         ->
        self.requests = []
        self.responses = []
        self.interfaceController = InterfaceController()
        request = SNAPRequest(path="api/parameters", payload="calibration/assessment")
        self.assessmentSchema = self.interfaceController.executeRequest(request).data
        # for each key, read string and convert to json
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        request = SNAPRequest(path="api/parameters", payload="calibration/save")
        self.saveSchema = self.interfaceController.executeRequest(request).data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close

        request = SNAPRequest(path="config/samplePaths")
        self.samplePaths = self.interfaceController.executeRequest(request).data

        request = SNAPRequest(path="config/groupingFiles")
        self.groupingFiles = self.interfaceController.executeRequest(request).data

        self._calibrationReductionView = CalibrationReductionRequestView(
            jsonForm, samples=self.samplePaths, groups=self.groupingFiles, parent=parent
        )
        self._calibrationAssessmentView = CalibrationAssessmentView(
            "Assessing Calibration", self.assessmentSchema, samples=self.samplePaths, parent=parent
        )
        self._saveCalibrationView = SaveCalibrationView("Saving Calibration", self.saveSchema, parent)

        self.workflow = (
            WorkflowBuilder(cancelLambda=cancelLambda, parent=parent)
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
            .addNode(self._saveCalibration, self._saveCalibrationView, "Saving")
            .build()
        )

    def _triggerCalibrationReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration reduction

        # TODO: prepopulate next run number
        self.runNumber = view.getFieldText("runNumber")
        sampleIndex = view.sampleDropdown.currentIndex()

        self._calibrationAssessmentView.updateSample(sampleIndex)
        self._calibrationAssessmentView.updateRunNumber(self.runNumber)
        self._saveCalibrationView.updateRunNumber(self.runNumber)

        payload = RunConfig(runNumber=self.runNumber)
        request = SNAPRequest(path="calibration/reduction", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def _assessCalibration(self, workflowPresenter):
        # TODO Load Previous ->
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration assessment
        runNumber = view.fieldRunNumber.text()
        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=runNumber), cifPath=view.sampleDropdown.currentText()
        )
        request = SNAPRequest(path="calibration/assessment", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration save
        calibrationRecord = CalibrationRecord(**self.responses[-1].data)
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=view.getFieldText("calibrationIndexEntry.runNumber"),
            comments=view.getFieldText("calibrationIndexEntry.comments"),
            author=view.getFieldText("calibrationIndexEntry.author"),
        )
        payload = CalibrationExportRequest(
            calibrationRecord=calibrationRecord, calibrationIndexEntry=calibrationIndexEntry
        )
        request = SNAPRequest(path="calibration/save", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        # wrap workflow.presenter.widget in a QMainWindow
        # show the QMainWindow
        pass

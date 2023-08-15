import json

from PyQt5.QtWidgets import QLabel

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.dao.calibration import CalibrationIndexEntry, CalibrationRecord
from snapred.backend.dao.request import CalibrationAssessmentRequest, CalibrationExportRequest
from snapred.ui.view.CalibrationReductionRequestView import CalibrationReductionRequestView
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


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

        self.workflow = (
            WorkflowBuilder(parent)
            .addNode(
                self._triggerCalibrationReduction,
                CalibrationReductionRequestView(jsonForm, parent=parent),
                "Calibrating",
            )
            .addNode(
                self._assessCalibration,
                JsonFormList("Assessing Calibration", self.assessmentSchema, parent).widget,
                "Assessing",
            )
            .addNode(
                self._saveCalibration, JsonFormList("Saving Calibration", self.saveSchema, parent).widget, "Saving"
            )
            .build()
        )

    def _triggerCalibrationReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration reduction
        payload = RunConfig(runNumber=view.getFieldText("runNumber"))
        request = SNAPRequest(path="calibration/reduction", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

    def _assessCalibration(self, workflowPresenter):
        # TODO Load Previous ->
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration assessment
        runNumber = view.getFieldText("run.runNumber")
        payload = CalibrationAssessmentRequest(run=RunConfig(runNumber=runNumber), cifPath=view.getFieldText("cifPath"))
        request = SNAPRequest(path="calibration/assessment", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

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

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        # wrap workflow.presenter.widget in a QMainWindow
        # show the QMainWindow
        pass

import json

from qtpy.QtWidgets import QLabel

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.dao.calibration import CalibrationIndexEntry, CalibrationRecord
from snapred.backend.dao.request import (
    CalibrationAssessmentRequest,
    CalibrationExportRequest,
    DiffractionCalibrationRequest,
)
from snapred.backend.dao.SNAPResponse import SNAPResponse
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
        self.focusGroups = self.interfaceController.executeRequest(request).data
        self.groupingFiles = list(self.focusGroups.keys())

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
        try:
            view.verify()
        except ValueError as e:
            return SNAPResponse(code=500, message=f"Missing Fields!{e}")

        self.runNumber = view.getFieldText("runNumber")
        sampleIndex = view.sampleDropdown.currentIndex()

        self._calibrationAssessmentView.updateSample(sampleIndex)
        self._calibrationAssessmentView.updateRunNumber(self.runNumber)
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

        request = SNAPRequest(path="calibration/diffraction", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def _assessCalibration(self, workflowPresenter):  # noqa: ARG002
        # TODO Load Previous ->
        # pull fields from view for calibration assessment
        payload = CalibrationAssessmentRequest(
            run=RunConfig(runNumber=self.runNumber),
            workspace=self.responses[-1].data["outputWorkspace"],
            focusGroup=self.focusGroups[self.focusGroupPath],
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            useLiteMode=self.useLiteMode,
            calibrantSamplePath=self.calibrantSamplePath,
        )
        request = SNAPRequest(path="calibration/assessment", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def _saveCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for calibration save
        calibrationRecord = self.responses[-1].data
        calibrationRecord.workspaceNames.append(self.responses[-2].data["calibrationTable"])
        calibrationIndexEntry = CalibrationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
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

import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest, SNAPResponse

# from snapred.ui.view.SaveNormalizationCalibrationView import SaveNormalizationCalibrationView
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.request import (
    #  NormalizationExportRequest,
    NormalizationCalibrationRequest,
    SpecifyNormalizationRequest,
)
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.NormalizationCalibrationRequestView import NormalizationCalibrationRequestView
from snapred.ui.view.SpecifyNormalizationCalibrationView import SpecifyNormalizationCalibrationView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


class NormalizationCalibrationWorkflow:
    def __init__(self, jsonForm, parent=None):
        self.requests = []
        self.responses = []
        self.interfaceController = InterfaceController()
        request = SNAPRequest(path="api/parameters", payload="calibration/normalizationAssessment")
        self.assessmentSchema = self.interfaceController.executeRequest(request).data
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        request = SNAPRequest(path="api/parameters", payload="calibration/saveNormalization")
        self.saveSchema = self.interfaceController.executeRequest(request).data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close

        request = SNAPRequest(path="config/samplePaths")
        self.samplePaths = self.interfaceController.executeRequest(request).data

        request = SNAPRequest(path="config/groupingFiles")
        self.groupingFiles = self.interfaceController.executeRequest(request).data

        self._normalizationCalibrationView = NormalizationCalibrationRequestView(
            jsonForm,
            self.samplePaths,
            self.groupingFiles,
            parent=parent,
        )

        self._specifyNormalizationView = SpecifyNormalizationCalibrationView(
            "Specifying Normalization",
            self.assessmentSchema,
            samples=self.samplePaths,
            groups=self.groupingFiles,
            parent=parent,
        )

        self._specifyNormalizationView.signalValueChanged.connect(self.onNormalizationValueChange)

        # self._saveNormalizationCalibrationView = SaveNormalizationCalibrationView(
        #     "Saving Normalization Calibration", self.saveSchema, parent
        # )

        self.workflow = (
            WorkflowBuilder(cancelLambda=cancelLambda, parent=parent)
            .addNode(
                self._triggerNormalizationCalibration,
                self._normalizationCalibrationView,
                "Normalization Calibration",
            )
            .addNode(
                self._specifyNormalization,
                self._specifyNormalizationView,
                "Specify Calibration",
            )
            # .addNode(self._saveNormalizationCalibration, self._saveNormalizationCalibrationView, "Saving")
            .build()
        )

    def _triggerNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        try:
            view.verify()
        except ValueError as e:
            return SNAPResponse(code=500, message=f"Missing Fields!{e}")

        self.runNumber = view.getFieldText("runNumber")
        self.backgroundRunNumber = view.getFieldText("backgroundRunNumber")
        smoothingParameter = view.getFieldText("smoothingParameter")
        self.sampleIndex = view.sampleDropDown.currentIndex()
        groupingIndex = view.groupingFileDropDown.currentIndex()
        self.samplePath = view.sampleDropDown.currentText()
        self.groupingPath = view.groupingFileDropDown.currentText()

        self._specifyNormalizationView.updateSample(self.sampleIndex)
        self._specifyNormalizationView.updateRunNumber(self.runNumber)
        self._specifyNormalizationView.updateBackgroundRunNumber(self.backgroundRunNumber)
        self._specifyNormalizationView.updateGrouping(groupingIndex)

        # self._saveNormalizationCalibrationView.updateSample(sampleIndex)
        # self._saveNormalizationCalibrationView.updateRunNumber(self.runNumber)
        # self._saveNormalizationCalibrationView.updateBackgroundRunNumber(self.backgroundRunNumber)
        # self._saveNormalizationCalibrationView.updateGroupingFile(groupingIndex)
        # self._saveNormalizationCalibrationView.updateCalibrantSample(calibrantIndex)
        # self._saveNormalizationCalibrationView.updateSmoothingParameter(self.smoothingParmameter)

        focusWS, smoothWS = self.callNormalizationCalibration(groupingIndex, smoothingParameter)
        self._specifyNormalizationView.updateWorkspaces(focusWS, smoothWS)

    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        pass

        # groupPath = view.groupingFileDropDown.currentText()
        # groupName = groupPath.split("/")[-1]
        # send focusWorkspace to recipe to clone and smooth
        # smoothedWorkspaces = self.createSmoothedWorkspaces(self.runNumber, focusWorkspaces, self.smoothingParameter)

        # self._specifyNormalizationView.signalWorkspacesUpdate.emit(focusWorkspace, smoothedWorkspaces)

        # payload = SpecifyNormalizationRequest(
        #     run=RunConfig(runNumber=self.runNumber),
        #     workspace=,
        #     smoothWorkspace=,
        #     smoothingParameter=#TODO: Pull this from the view.,
        #     samplePath=self.samplePath,
        #     calibrantPath=self.calibrantPath,
        #     focusGroupPath=self.groupingPath,
        # )
        # request = SNAPRequest(path="calibration/normalizationAssessment", payload=payload.json())
        # response = self.interfaceController.executeRequest(request)

        # self.responses.append(response)
        # return response

    # def _saveNormalizationCalibration(self, workflowPresenter):
    #     view = workflowPresenter.widget.tabview

    #     normalizationRecord = self.responses[-1].data
    #     normalizationRecord.workspaceNames.append(self.responses[-2].data)
    #     pass

    def callNormalizationCalibration(self, groupingIndex, smoothingParameter):
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            samplePath=self.samplePath,
            groupingPath=self.groupingFiles[groupingIndex],
            smoothingParameter=smoothingParameter,
        )

        request = SNAPRequest(path="calibration/normalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

        focusWorkspace = self.responses[-1].data["FocusWorkspace"]
        smoothWorkspace = self.responses[-1].data["SmoothWorkspace"]

        return focusWorkspace, smoothWorkspace

    def onNormalizationValueChange(self, index, smoothingValue):
        # self._saveNormalizationCalibrationView.updateCalibrantSample(index)
        # self._saveNormalizationCalibrationView.updateSmoothingParameter(smoothingValue)
        focusWS, smoothWS = self.callNormalizationCalibration(index, smoothingValue)
        self._specifyNormalizationView.updateWorkspaces(focusWS, smoothWS)

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        pass

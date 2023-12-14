import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.normalization import NormalizationIndexEntry, NormalizationRecord
from snapred.backend.dao.request import (
    NormalizationCalibrationRequest,
    NormalizationExportRequest,
)
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.NormalizationCalibrationRequestView import NormalizationCalibrationRequestView
from snapred.ui.view.SaveNormalizationCalibrationView import SaveNormalizationCalibrationView
from snapred.ui.view.SpecifyNormalizationCalibrationView import SpecifyNormalizationCalibrationView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


class NormalizationCalibrationWorkflow:
    def __init__(self, jsonForm, parent=None):
        self.requests = []
        self.responses = []
        self.initializationComplete = False
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

        self.lastGroupingFile = None
        self.lastSmoothingParameter = None

        self._specifyNormalizationView.signalValueChanged.connect(self.onNormalizationValueChange)

        self._saveNormalizationCalibrationView = SaveNormalizationCalibrationView(
            "Saving Normalization Calibration",
            self.saveSchema,
            parent,
        )

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
                "Tweak Parameters",
            )
            .addNode(self._saveNormalizationCalibration, self._saveNormalizationCalibrationView, "Saving")
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
        self.sampleIndex = (view.sampleDropDown.currentIndex()) - 1
        self.initGroupingIndex = (view.groupingFileDropDown.currentIndex()) - 1
        self.initSmoothingParameter = float(view.getFieldText("smoothingParameter"))
        self.samplePath = view.sampleDropDown.currentText()
        self.groupingPath = view.groupingFileDropDown.currentText()

        self._specifyNormalizationView.updateFields(
            sampleIndex=self.sampleIndex,
            groupingIndex=self.initGroupingIndex,
            smoothingParameter=self.initSmoothingParameter,
        )

        self._specifyNormalizationView.updateRunNumber(self.runNumber)
        self._specifyNormalizationView.updateBackgroundRunNumber(self.backgroundRunNumber)

        self._saveNormalizationCalibrationView.updateRunNumber(self.runNumber)
        self._saveNormalizationCalibrationView.updateBackgroundRunNumber(self.backgroundRunNumber)

        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            samplePath=str(self.samplePaths[self.sampleIndex]),
            groupingPath=str(self.groupingFiles[self.initGroupingIndex]),
            smoothingParameter=self.initSmoothingParameter,
        )

        request = SNAPRequest(path="calibration/normalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

        focusWorkspace = self.responses[-1].data["outputWorkspace"]
        smoothWorkspace = self.responses[-1].data["smoothedOutput"]

        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)
        self.initializationComplete = True
        return response

    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        if self.lastGroupingFile is not None and self.lastSmoothingParameter is not None:
            payload = NormalizationCalibrationRequest(
                runNumber=self.runNumber,
                backgroundRunNumber=self.backgroundRunNumber,
                samplePath=str(self.samplePaths[self.sampleIndex]),
                groupingPath=str(self.lastGroupingFile),
                smoothingParameter=self.lastSmoothingParameter,
            )
        else:
            payload = NormalizationCalibrationRequest(
                runNumber=self.runNumber,
                backgroundRunNumber=self.backgroundRunNumber,
                samplePath=str(self.samplePaths[self.sampleIndex]),
                groupingPath=str(self.groupingFiles[self.initGroupingIndex]),
                smoothingParameter=self.initSmoothingParameter,
            )

        request = SNAPRequest(path="calibration/normalizationAssessment", payload=payload.json())
        response = self.interfaceController.executeRequest(request)

        self.responses.append(response)
        return response

    def _saveNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        normalizationRecord = self.responses[-1].data
        normalizationIndexEntry = NormalizationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            backgroundRunNumber=view.fieldBackgroundRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
        )
        payload = NormalizationExportRequest(
            normalizationRecord=normalizationRecord, normalizationIndexEntry=normalizationIndexEntry
        )
        request = SNAPRequest(path="calibration/saveNormalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def callNormalizationCalibration(self, groupingFile, smoothingParameter):
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            samplePath=self.samplePaths[self.sampleIndex],
            groupingPath=groupingFile,
            smoothingParameter=smoothingParameter,
        )

        request = SNAPRequest(path="calibration/normalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

        focusWorkspace = self.responses[-1].data["outputWorkspace"]
        smoothWorkspace = self.responses[-1].data["smoothedOutput"]

        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)

    def onNormalizationValueChange(self, index, smoothingValue):  # noqa: ARG002
        if not self.initializationComplete:
            return

        self.lastGroupingFile = self.groupingFiles[index]
        self.lastSmoothingParameter = smoothingValue

        groupingFileChanged = self.groupingFiles[self.initGroupingIndex] != self.lastGroupingFile
        smoothingValueChanged = self.initSmoothingParameter != self.lastSmoothingParameter

        if groupingFileChanged or smoothingValueChanged:
            from mantid.simpleapi import DeleteWorkspace

            DeleteWorkspace(Workspace="focussedRawVanadium")
            DeleteWorkspace(Workspace="smoothedOutput")
            self.initGroupingIndex = index
            self.initSmoothingParameter = smoothingValue
            self.callNormalizationCalibration(self.groupingFiles[index], smoothingValue)
        else:
            if "outputWorkspace" in self.responses[-1].data and "smoothedOutput" in self.responses[-1].data:
                focusWorkspace = self.responses[-1].data["outputWorkspace"]
                smoothWorkspace = self.responses[-1].data["smoothedOutput"]
                self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)
            else:
                raise Exception("Expected data not found in the last response")

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        pass

import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.dao.normalization import NormalizationIndexEntry, NormalizationRecord
from snapred.backend.dao.request import (
    NormalizationCalibrationRequest,
    NormalizationExportRequest,
)
from snapred.backend.dao.request.SmoothDataExcludingPeaksRequest import SmoothDataExcludingPeaksRequest
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
        request = SNAPRequest(path="api/parameters", payload="normalization/assessment")
        self.assessmentSchema = self.interfaceController.executeRequest(request).data
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        request = SNAPRequest(path="api/parameters", payload="normalization/save")
        self.saveSchema = self.interfaceController.executeRequest(request).data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close

        request = SNAPRequest(path="config/samplePaths")
        self.samplePaths = self.interfaceController.executeRequest(request).data

        request = SNAPRequest(path="config/focusGroups")
        self.focusGroups = self.interfaceController.executeRequest(request).data
        self.groupingFiles = list(self.focusGroups.keys())

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
        self.initDMin = float(self._specifyNormalizationView.fielddMin.field.text())

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
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=self.focusGroups[str(self.groupingFiles[self.initGroupingIndex])],
            smoothingParameter=self.initSmoothingParameter,
            dMin=self.initDMin,
        )

        request = SNAPRequest(path="normalization", payload=payload.json())
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
                calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
                focusGroup=self.focusGroups[str(self.lastGroupingFile)],
                smoothingParameter=self.lastSmoothingParameter,
                dMin=self.lastDMin,
            )
        else:
            payload = NormalizationCalibrationRequest(
                runNumber=self.runNumber,
                backgroundRunNumber=self.backgroundRunNumber,
                calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
                focusGroup=self.focusGroups[str(self.groupingFiles[self.initGroupingIndex])],
                smoothingParameter=self.initSmoothingParameter,
                dMin=self.initDMin,
            )

        request = SNAPRequest(path="normalization/assessment", payload=payload.json())
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
        request = SNAPRequest(path="normalization/save", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response

    def callNormalizationCalibration(self, groupingFile, smoothingParameter, dMin):
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=self.focusGroups[groupingFile],
            smoothingParameter=smoothingParameter,
            dMin=dMin,
        )

        request = SNAPRequest(path="normalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)

        focusWorkspace = self.responses[-1].data["outputWorkspace"]
        smoothWorkspace = self.responses[-1].data["smoothedOutput"]

        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)

    def applySmoothingUpdate(self, index, smoothingValue, dMin):
        workspaces = self.responses[-1].data
        payload = SmoothDataExcludingPeaksRequest(
            inputWorkspace=workspaces["outputWorkspace"],
            outputWorkspace=workspaces["smoothedOutput"],
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=self.focusGroups[self.groupingFiles[index]],
            runNumber=self.runNumber,
            smoothingParameter=smoothingValue,
            dMin=dMin,
        )
        request = SNAPRequest(path="normalization/smooth", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        focusWorkspace = workspaces["outputWorkspace"]
        smoothWorkspace = response.data
        self.responses[-1].data["smoothedOutput"] = response.data
        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)

    def onNormalizationValueChange(self, index, smoothingValue, dMin):  # noqa: ARG002
        if not self.initializationComplete:
            return
        # disable recalculate button
        self._specifyNormalizationView.disableRecalculateButton()

        self.lastGroupingFile = self.groupingFiles[index]
        self.lastSmoothingParameter = smoothingValue
        self.lastDMin = dMin

        groupingFileChanged = self.groupingFiles[self.initGroupingIndex] != self.lastGroupingFile
        smoothingValueChanged = self.initSmoothingParameter != self.lastSmoothingParameter
        dMinValueChanged = self.initDMin != self.lastDMin
        if groupingFileChanged:
            self.initGroupingIndex = index
            self.initSmoothingParameter = smoothingValue
            self.callNormalizationCalibration(self.groupingFiles[index], smoothingValue, dMin)
        elif smoothingValueChanged or dMinValueChanged:
            self.applySmoothingUpdate(index, smoothingValue, dMin)
        elif "outputWorkspace" in self.responses[-1].data and "smoothedOutput" in self.responses[-1].data:
            focusWorkspace = self.responses[-1].data["outputWorkspace"]
            smoothWorkspace = self.responses[-1].data["smoothedOutput"]
            self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace)
        else:
            raise Exception("Expected data not found in the last response")
        self._specifyNormalizationView.enableRecalculateButton()

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        pass

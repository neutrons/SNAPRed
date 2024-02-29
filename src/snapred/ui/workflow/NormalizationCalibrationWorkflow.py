import json

from pydantic import parse_raw_as
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
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class NormalizationCalibrationWorkflow(WorkflowImplementer):
    def __init__(self, jsonForm, parent=None):
        super().__init__(parent)

        self.assessmentSchema = self.request(path="api/parameters", payload="normalization/assessment").data
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        self.saveSchema = self.request(path="api/parameters", payload="normalization/save").data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}

        self.samplePaths = self.request(path="config/samplePaths").data

        self._normalizationCalibrationView = NormalizationCalibrationRequestView(
            jsonForm,
            samplePaths=self.samplePaths,
            parent=parent,
        )

        self._specifyNormalizationView = SpecifyNormalizationCalibrationView(
            jsonForm,
            samples=self.samplePaths,
            parent=parent,
        )

        self._specifyNormalizationView.signalValueChanged.connect(self.onNormalizationValueChange)

        self._saveNormalizationCalibrationView = SaveNormalizationCalibrationView(
            "Saving Normalization Calibration",
            self.saveSchema,
            parent,
        )

        # connect signal to populate the grouping dropdown after run is selected
        self._normalizationCalibrationView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._specifyNormalizationView.fieldRunNumber.editingFinished.connect(self._populateGroupingDropdown)

        self.workflow = (
            WorkflowBuilder(cancelLambda=None, parent=parent)
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

    def _populateGroupingDropdown(self):
        # when the run number is updated, grab the grouping map and populate grouping drop down
        runNumber = self._normalizationCalibrationView.runNumberField.text()
        useLiteMode = self._normalizationCalibrationView.litemodeToggle.field.getState()
        self._normalizationCalibrationView.groupingFileDropDown.setEnabled(False)

        # pre-screen the run number to make sure it is complete before any further checks
        if len(runNumber) < 5:
            return

        # check if the state exists -- if so load its grouping map
        hasState = self.request(path="calibration/hasState", payload=runNumber).data
        if hasState:
            response = self.request(path="config/groupingMap", payload=runNumber).data
            self.focusGroups = response.getMap(useLiteMode)
            self.groupingFiles = list(self.focusGroups.keys())
            self._normalizationCalibrationView.populateGroupingDropdown(self.groupingFiles)
            self._specifyNormalizationView.populateGroupingDropdown(self.groupingFiles)
        else:
            logger.warn(f"Could not find state folder for run {runNumber} -- verify run number field")

    def _triggerNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        try:
            view.verify()
        except ValueError as e:
            return SNAPResponse(code=500, message=f"Missing Fields!{e}")

        self.runNumber = view.getFieldText("runNumber")
        self.backgroundRunNumber = view.getFieldText("backgroundRunNumber")
        self.sampleIndex = view.sampleDropDown.currentIndex()
        self.prevGroupingIndex = view.groupingFileDropDown.currentIndex()
        self.samplePath = view.sampleDropDown.currentText()
        self.groupingPath = view.groupingFileDropDown.currentText()
        self.prevDMin = float(self._specifyNormalizationView.fielddMin.field.text())
        self.prevDMax = float(self._specifyNormalizationView.fielddMax.field.text())
        self.prevThreshold = float(self._specifyNormalizationView.fieldThreshold.field.text())

        # init the payload
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=self.focusGroups[str(self.groupingFiles[self.prevGroupingIndex])],
            crystalDMin=self.prevDMin,
        )
        # take the default smoothing param from the default payload value
        self.prevSmoothingParameter = payload.smoothingParameter

        self._specifyNormalizationView.updateFields(
            sampleIndex=self.sampleIndex,
            groupingIndex=self.prevGroupingIndex,
            smoothingParameter=self.prevSmoothingParameter,
        )

        self._specifyNormalizationView.updateRunNumber(self.runNumber)
        self._specifyNormalizationView.updateBackgroundRunNumber(self.backgroundRunNumber)

        self._saveNormalizationCalibrationView.updateRunNumber(self.runNumber)
        self._saveNormalizationCalibrationView.updateBackgroundRunNumber(self.backgroundRunNumber)

        response = self.request(path="normalization", payload=payload.json())
        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
        peaks = self.responses[-1].data["detectorPeaks"]

        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)
        self.initializationComplete = True
        return response

    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=self.focusGroups[str(self.groupingFiles[self.prevGroupingIndex])],
            smoothingParameter=self.prevSmoothingParameter,
            crystalDMin=self.prevDMin,
            crystalDMax=self.prevDMax,
            peakIntensityThreshold=self.prevThreshold,
        )
        response = self.request(path="normalization/assessment", payload=payload.json())
        return response

    def _saveNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        normalizationRecord = self.responses[-1].data
        normalizationRecord.workspaceNames.append(self.responses[-2].data["smoothedVanadium"])
        normalizationRecord.workspaceNames.append(self.responses[-2].data["focusedVanadium"])
        normalizationRecord.workspaceNames.append(self.responses[-2].data["correctedVanadium"])

        normalizationIndexEntry = NormalizationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            backgroundRunNumber=view.fieldBackgroundRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
        )

        payload = NormalizationExportRequest(
            normalizationRecord=normalizationRecord,
            normalizationIndexEntry=normalizationIndexEntry,
        )
        response = self.request(path="normalization/save", payload=payload.json())
        self.responses.append(response)

        return response

    def callNormalizationCalibration(self, index, smoothingParameter, dMin, dMax, peakThreshold):
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=self.focusGroups[self.groupingFiles[index]],
            smoothingParameter=smoothingParameter,
            crystalDMin=dMin,
            crystalDMax=dMax,
            peakIntensityThreshold=peakThreshold,
        )
        self.request(path="normalization", payload=payload.json())

        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
        peaks = self.responses[-1].data["detectorPeaks"]
        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)

    def applySmoothingUpdate(self, index, smoothingValue, dMin, dMax, peakThreshold):
        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
        payload = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focusWorkspace,
            outputWorkspace=smoothWorkspace,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=self.focusGroups[self.groupingFiles[index]],
            runNumber=self.runNumber,
            smoothingParameter=smoothingValue,
            crystalDMin=dMin,
            crystalDMax=dMax,
            peakIntensityThreshold=peakThreshold,
        )
        response = self.request(path="normalization/smooth", payload=payload.json())

        peaks = response.data["detectorPeaks"]
        self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)

    def onNormalizationValueChange(self, index, smoothingValue, dMin, dMax, peakThreshold):  # noqa: ARG002
        if not self.initializationComplete:
            return
        # disable recalculate button
        self._specifyNormalizationView.disableRecalculateButton()

        # if the grouping file change, redo whole calculation
        groupingFileChanged = index != self.prevGroupingIndex
        # if peaks will change, redo only the smoothing
        smoothingValueChanged = self.prevSmoothingParameter != smoothingValue
        dMinValueChanged = dMin != self.prevDMin
        dMaxValueChanged = dMax != self.prevDMax
        thresholdChanged = peakThreshold != self.prevThreshold
        peakListWillChange = smoothingValueChanged or dMinValueChanged or dMaxValueChanged or thresholdChanged

        # check the case, apply correct update
        if groupingFileChanged:
            self.callNormalizationCalibration(index, smoothingValue, dMin, dMax, peakThreshold)
        elif peakListWillChange:
            self.applySmoothingUpdate(index, smoothingValue, dMin, dMax, peakThreshold)
        elif "focusedVanadium" in self.responses[-1].data and "smoothedVanadium" in self.responses[-1].data:
            # if nothing changed but this function was called anyway... just replot stuff with old values
            focusWorkspace = self.responses[-1].data["focusedVanadium"]
            smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
            peaks = self.responses[-1].data["detectorPeaks"]
            self._specifyNormalizationView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)
        else:
            raise Exception("Expected data not found in the last response")

        # renable button when graph is updated
        self._specifyNormalizationView.enableRecalculateButton()

        # update the values for next call to this method
        self.prevGroupingIndex = index
        self.prevSmoothingParameter = smoothingValue
        self.prevDMin = dMin
        self.prevDMax = dMax
        self.prevThreshold = peakThreshold

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        pass

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
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class NormalizationWorkflow(WorkflowImplementer):
    def __init__(self, jsonForm, parent=None):
        super().__init__(parent)

        self.initializationComplete = False

        # TODO enable set by toggle
        self.useLiteMode = True

        self.assessmentSchema = self.request(path="api/parameters", payload="normalization/assessment").data
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema.items()}

        self.saveSchema = self.request(path="api/parameters", payload="normalization/save").data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}

        self.samplePaths = self.request(path="config/samplePaths").data
        self.defaultGroupingMap = self.request(path="config/groupingMap", payload="tmfinr").data
        self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(self.useLiteMode)

        self._requestView = NormalizationRequestView(
            jsonForm,
            samplePaths=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._tweakPeakView = NormalizationTweakPeakView(
            jsonForm,
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._saveView = NormalizationSaveView(
            "Saving Normalization Calibration",
            self.saveSchema,
            parent,
        )

        # connect signal to populate the grouping dropdown after run is selected
        self._requestView.litemodeToggle.field.connectUpdate(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._tweakPeakView.signalValueChanged.connect(self.onNormalizationValueChange)

        self.workflow = (
            WorkflowBuilder(cancelLambda=None, parent=parent)
            .addNode(self._triggerNormalizationCalibration, self._requestView, "Normalization Calibration")
            .addNode(self._specifyNormalization, self._tweakPeakView, "Tweak Parameters")
            .addNode(self._saveNormalizationCalibration, self._saveView, "Saving")
            .build()
        )

    def _populateGroupingDropdown(self):
        # when the run number is updated, grab the grouping map and populate grouping drop down
        runNumber = self._requestView.runNumberField.text()
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.litemodeToggle.setEnabled(False)
        self._requestView.groupingFileDropdown.setEnabled(False)

        # check if the state exists -- if so load its grouping map
        hasState = self.request(path="calibration/hasState", payload=runNumber).data
        if hasState:
            self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
        else:
            self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(useLiteMode)

        # populate and reenable the drop down
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._requestView.litemodeToggle.setEnabled(True)
        self._requestView.groupingFileDropdown.setEnabled(True)

    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        self.focusGroups = self.groupingMap.getMap(useLiteMode)
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        self._requestView.groupingFileDropdown.setEnabled(True)

    def _triggerNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for normalization
        self.verifyForm(view)

        self.runNumber = view.getFieldText("runNumber")
        self.backgroundRunNumber = view.getFieldText("backgroundRunNumber")
        self.sampleIndex = view.sampleDropdown.currentIndex()
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()
        self.samplePath = view.sampleDropdown.currentText()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.prevDMin = float(self._tweakPeakView.fielddMin.field.text())
        self.prevDMax = float(self._tweakPeakView.fielddMax.field.text())
        self.prevThreshold = float(self._tweakPeakView.fieldThreshold.field.text())

        # init the payload
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=self.focusGroups[self.focusGroupPath],
            crystalDMin=self.prevDMin,
        )
        # take the default smoothing param from the default payload value
        self.prevSmoothingParameter = payload.smoothingParameter

        # populate fields in future views
        self._tweakPeakView.populateGroupingDropdown(list(self.groupingMap.getMap(self.useLiteMode).keys()))
        self._tweakPeakView.updateFields(
            sampleIndex=self.sampleIndex,
            groupingIndex=self.prevGroupingIndex,
            smoothingParameter=self.prevSmoothingParameter,
        )
        self._tweakPeakView.updateRunNumber(self.runNumber)
        self._tweakPeakView.updateBackgroundRunNumber(self.backgroundRunNumber)

        self._saveView.updateRunNumber(self.runNumber)
        self._saveView.updateBackgroundRunNumber(self.backgroundRunNumber)

        response = self.request(path="normalization", payload=payload.json())
        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
        peaks = self.responses[-1].data["detectorPeaks"]

        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)
        self.initializationComplete = True
        return response

    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=list(self.focusGroups.items())[self.prevGroupingIndex][1],
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
            appliesTo=view.fieldAppliesTo.get(),
        )

        payload = NormalizationExportRequest(
            normalizationRecord=normalizationRecord,
            normalizationIndexEntry=normalizationIndexEntry,
        )
        response = self.request(path="normalization/save", payload=payload.json())
        return response

    def callNormalizationCalibration(self, index, smoothingParameter, dMin, dMax, peakThreshold):
        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=list(self.focusGroups.items())[index][1],
            smoothingParameter=smoothingParameter,
            crystalDMin=dMin,
            crystalDMax=dMax,
            peakIntensityThreshold=peakThreshold,
        )
        self.request(path="normalization", payload=payload.json())

        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
        peaks = self.responses[-1].data["detectorPeaks"]
        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)

    def applySmoothingUpdate(self, index, smoothingValue, dMin, dMax, peakThreshold):
        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]

        payload = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focusWorkspace,
            outputWorkspace=smoothWorkspace,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=list(self.focusGroups.items())[index][1],
            runNumber=self.runNumber,
            smoothingParameter=smoothingValue,
            crystalDMin=dMin,
            crystalDMax=dMax,
            peakIntensityThreshold=peakThreshold,
        )
        response = self.request(path="normalization/smooth", payload=payload.json())

        peaks = response.data["detectorPeaks"]
        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)

    def onNormalizationValueChange(self, index, smoothingValue, dMin, dMax, peakThreshold):  # noqa: ARG002
        if not self.initializationComplete:
            return
        # disable recalculate button
        self._tweakPeakView.disableRecalculateButton()

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
            self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks)
        else:
            raise Exception("Expected data not found in the last response")

        # renable button when graph is updated
        self._tweakPeakView.enableRecalculateButton()

        # update the values for next call to this method
        self.prevGroupingIndex = index
        self.prevSmoothingParameter = smoothingValue
        self.prevDMin = dMin
        self.prevDMax = dMax
        self.prevThreshold = peakThreshold

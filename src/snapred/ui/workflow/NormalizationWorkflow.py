from snapred.backend.dao.normalization import NormalizationIndexEntry
from snapred.backend.dao.request import (
    HasStateRequest,
    NormalizationExportRequest,
    NormalizationRequest,
)
from snapred.backend.dao.request.SmoothDataExcludingPeaksRequest import SmoothDataExcludingPeaksRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.EntryExitLogger import EntryExitLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.ui.view.NormalizationRequestView import NormalizationRequestView
from snapred.ui.view.NormalizationSaveView import NormalizationSaveView
from snapred.ui.view.NormalizationTweakPeakView import NormalizationTweakPeakView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class NormalizationWorkflow(WorkflowImplementer):
    """

    This system orchestrates a full workflow for scientific data normalization, guiding users through each step with
    interactive qt widgets and custom views. Starting with default settings for initialization, it progresses
    through calibration, parameter adjustments, and ends with saving normalization data, offering views like
    NormalizationRequestView, NormalizationTweakPeakView, and NormalizationSaveView for an interactive user workflow.
    The workflow dynamically adjusts to different datasets and requirements, ensuring adaptability. Key phases include
    capturing initial user inputs, interactive parameter tweaking with real-time visualization, and collecting final
    details for data saving. This approach not only ensures a responsive and user-friendly experience but also maintains
    workflow flexibility and data integrity through comprehensive validation and error handling mechanisms.

    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.initializationComplete = False

        self.samplePaths = self.request(path="config/samplePaths").data
        self.defaultGroupingMap = self.request(path="config/groupingMap", payload="tmfinr").data
        self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.lite

        self._requestView = NormalizationRequestView(
            samplePaths=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._tweakPeakView = NormalizationTweakPeakView(
            samples=self.samplePaths,
            groups=list(self.focusGroups.keys()),
            parent=parent,
        )
        self._saveView = NormalizationSaveView(parent)

        # connect signal to populate the grouping dropdown after run is selected
        self._requestView.litemodeToggle.field.connectUpdate(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._tweakPeakView.signalValueChanged.connect(self.onNormalizationValueChange)

        self.workflow = (
            WorkflowBuilder(cancelLambda=None, parent=parent)
            .addNode(self._triggerNormalization, self._requestView, "Normalization Calibration")
            .addNode(self._specifyNormalization, self._tweakPeakView, "Tweak Parameters")
            .addNode(self._saveNormalization, self._saveView, "Saving")
            .build()
        )
        self.workflow.presenter.setResetLambda(self.reset)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    def _populateGroupingDropdown(self):
        # when the run number is updated, grab the grouping map and populate grouping drop down
        runNumber = self._requestView.runNumberField.text()
        self.useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.litemodeToggle.setEnabled(False)
        self._requestView.groupingFileDropdown.setEnabled(False)
        # TODO: Use threads, account for fail cases
        try:
            # check if the state exists -- if so load its grouping map
            payload = HasStateRequest(
                runId=runNumber,
                useLiteMode=self.useLiteMode,
            )
            hasState = self.request(path="calibration/hasState", payload=payload.json()).data
            # hasState = self.request(path="calibration/hasState", payload=runNumber).data
            if hasState:
                self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
            else:
                self.groupingMap = self.defaultGroupingMap
            self.focusGroups = self.groupingMap.getMap(self.useLiteMode)

            # populate and reenable the drop down
            self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        except Exception as e:  # noqa BLE001
            print(e)
        self._requestView.litemodeToggle.setEnabled(True)
        self._requestView.groupingFileDropdown.setEnabled(True)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._requestView.litemodeToggle.field.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)
        # TODO: Use threads, account for fail cases
        try:
            self.focusGroups = self.groupingMap.getMap(useLiteMode)
            self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        except Exception as e:  # noqa BLE001
            print(e)

        self._requestView.groupingFileDropdown.setEnabled(True)

    @EntryExitLogger(logger=logger)
    def _triggerNormalization(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for normalization

        self.runNumber = view.runNumberField.field.text()
        self.useLiteMode = view.litemodeToggle.field.getState()
        self.backgroundRunNumber = view.backgroundRunNumberField.field.text()
        self.sampleIndex = view.sampleDropdown.currentIndex()
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()
        self.samplePath = view.sampleDropdown.currentText()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.prevDMin = float(self._tweakPeakView.fielddMin.field.text())
        self.prevDMax = float(self._tweakPeakView.fielddMax.field.text())
        self.prevThreshold = float(self._tweakPeakView.fieldThreshold.field.text())

        # init the payload
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
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

    @EntryExitLogger(logger=logger)
    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
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

    @EntryExitLogger(logger=logger)
    def _saveNormalization(self, workflowPresenter):
        view = workflowPresenter.widget.tabView

        normalizationRecord = self.responses[-1].data
        normalizationRecord.workspaceNames.append(self.responses[-2].data["smoothedVanadium"])
        normalizationRecord.workspaceNames.append(self.responses[-2].data["focusedVanadium"])
        normalizationRecord.workspaceNames.append(self.responses[-2].data["correctedVanadium"])

        version = view.fieldVersion.get(None)
        # validate the version number
        version = view.fieldVersion.get(None)
        if version is not None:
            try:
                version = int(version)
                assert version >= 0
            except (AssertionError, ValueError, TypeError):
                raise TypeError("Version must be a nonnegative integer.")

        normalizationIndexEntry = NormalizationIndexEntry(
            runNumber=view.fieldRunNumber.get(),
            useLiteMode=self.useLiteMode,
            backgroundRunNumber=view.fieldBackgroundRunNumber.get(),
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
            appliesTo=view.fieldAppliesTo.get(),
        )

        payload = NormalizationExportRequest(
            version=version,
            normalizationRecord=normalizationRecord,
            normalizationIndexEntry=normalizationIndexEntry,
        )
        response = self.request(path="normalization/save", payload=payload.json())
        return response

    @EntryExitLogger(logger=logger)
    def callNormalization(self, index, smoothingParameter, dMin, dMax, peakThreshold):
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
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

    @EntryExitLogger(logger=logger)
    def applySmoothingUpdate(self, index, smoothingValue, dMin, dMax, peakThreshold):
        focusWorkspace = self.responses[-1].data["focusedVanadium"]
        smoothWorkspace = self.responses[-1].data["smoothedVanadium"]

        payload = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focusWorkspace,
            useLiteMode=self.useLiteMode,
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

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    def onNormalizationValueChange(self, index, smoothingValue, dMin, dMax, peakThreshold):  # noqa: ARG002
        if not self.initializationComplete:
            return
        # disable recalculate button
        self._tweakPeakView.disableRecalculateButton()
        # TODO: This is a temporary solution,
        # this should have never been setup to all run on the same thread.
        # It assumed an exception would never be tossed and thus
        # would never enable the recalc button again if one did
        try:
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
                self.callNormalization(index, smoothingValue, dMin, dMax, peakThreshold)
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

            # update the values for next call to this method
            self.prevGroupingIndex = index
            self.prevSmoothingParameter = smoothingValue
            self.prevDMin = dMin
            self.prevDMax = dMax
            self.prevThreshold = peakThreshold
        except Exception as e:  # noqa BLE001
            print(e)

        # renable button when graph is updated
        self._tweakPeakView.enableRecalculateButton()

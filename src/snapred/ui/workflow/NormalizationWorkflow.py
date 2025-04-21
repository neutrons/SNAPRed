from qtpy.QtCore import Slot

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VersionedObject, VersionState
from snapred.backend.dao.request import (
    CalculateNormalizationResidualRequest,
    CalibrationWritePermissionsRequest,
    CreateIndexEntryRequest,
    CreateNormalizationRecordRequest,
    HasStateRequest,
    NormalizationExportRequest,
    NormalizationRequest,
    RunMetadataRequest,
)
from snapred.backend.dao.request.SmoothDataExcludingPeaksRequest import SmoothDataExcludingPeaksRequest
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.EntryExitLogger import EntryExitLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.validator.RunNumberValidator import RunNumberValidator
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter
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
        self.normalizationResponse = None

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
        self._requestView.liteModeToggle.stateChanged.connect(self._switchLiteNativeGroups)
        self._requestView.runNumberField.editingFinished.connect(self._populateGroupingDropdown)
        self._requestView.backgroundRunNumberField.editingFinished.connect(self._verifyBackgroundRunNumber)
        self._tweakPeakView.signalValueChanged.connect(self.onNormalizationValueChange)

        self.workflow = (
            WorkflowBuilder(
                startLambda=self.start,
                resetLambda=self.reset,
                parent=parent,
            )
            .addNode(
                self._triggerNormalization,
                self._requestView,
                "Normalization Calibration",
                continueAnywayHandler=self._continueAnywayHandler,
            )
            .addNode(self._specifyNormalization, self._tweakPeakView, "Tweak Parameters")
            .addNode(self._saveNormalization, self._saveView, "Saving")
            .build()
        )

    def _setInteractive(self, state: bool):
        self._requestView.liteModeToggle.setEnabled(state)
        self._requestView.groupingFileDropdown.setEnabled(state)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    @Slot()
    def _populateGroupingDropdown(self):
        # when the run number is updated, grab the grouping map and populate grouping drop down
        runNumber = self._requestView.runNumberField.text()
        self.useLiteMode = self._requestView.liteModeToggle.getState()

        self._setInteractive(False)

        def _onDropdownSuccess():
            self._setInteractive(True)
            self._populateRunMetadata(runNumber)

        self.workflow.presenter.handleAction(
            self.handleDropdown,
            args=(runNumber, self.useLiteMode),
            onSuccess=_onDropdownSuccess,
        )

    @ExceptionToErrLog
    @Slot()
    def _populateRunMetadata(self, runNumber: str):
        if not bool(runNumber):
            self._requestView.updateRunMetadata()
            return
        self.workflow.presenter.handleAction(
            self.handleRunMetadata,
            args=(runNumber,),
            onSuccess=lambda: self._setInteractive(True),
        )

    def handleDropdown(self, runNumber, useLiteMode):
        # check if the state exists -- if so load its grouping map
        payload = HasStateRequest(
            runId=runNumber,
            useLiteMode=useLiteMode,
        )
        hasState = self.request(path="calibration/hasState", payload=payload.json()).data
        if hasState:
            self.groupingMap = self.request(path="config/groupingMap", payload=runNumber).data
        else:
            self.groupingMap = self.defaultGroupingMap
        self.focusGroups = self.groupingMap.getMap(useLiteMode)

        # populate and reenable the drop down
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        return SNAPResponse(code=ResponseCode.OK)

    def handleRunMetadata(self, runNumber):
        if not RunNumberValidator.validateRunNumber(runNumber):
            return SNAPResponse(code=ResponseCode.OK)
        payload = RunMetadataRequest(runId=runNumber)
        metadata = self.request(path="calibration/runMetadata", payload=payload.json()).data
        self._requestView.updateRunMetadata(metadata)
        return SNAPResponse(code=ResponseCode.OK)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    @Slot()
    def _verifyBackgroundRunNumber(self):
        backgroundRunNumber = self._requestView.backgroundRunNumberField.text()

        self.workflow.presenter.handleAction(
            self.verifyBackgroundRunNumber,
            args=(backgroundRunNumber,),
            onSuccess=lambda: self._requestView.groupingFileDropdown.setEnabled(True),
        )

    def verifyBackgroundRunNumber(self, backgroundRunNumber):
        if not RunNumberValidator.validateRunNumber(backgroundRunNumber):
            raise Exception(f"Invalid run number: {backgroundRunNumber}")
        return SNAPResponse(code=ResponseCode.OK)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    @Slot()
    def _switchLiteNativeGroups(self):
        # when the run number is updated, freeze the drop down to populate it
        useLiteMode = self._requestView.liteModeToggle.getState()

        self._requestView.groupingFileDropdown.setEnabled(False)

        self.workflow.presenter.handleAction(
            self.handleSwitchLiteNative,
            args=(useLiteMode,),
            onSuccess=lambda: self._requestView.groupingFileDropdown.setEnabled(True),
        )

    def handleSwitchLiteNative(self, useLiteMode):
        self.focusGroups = self.groupingMap.getMap(useLiteMode)
        self._requestView.populateGroupingDropdown(list(self.focusGroups.keys()))
        return SNAPResponse(code=ResponseCode.OK)

    @EntryExitLogger(logger=logger)
    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _triggerNormalization(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        # pull fields from view for normalization

        self.runNumber = view.runNumberField.field.text()
        self.useLiteMode = view.liteModeToggle.getState()
        self.backgroundRunNumber = view.backgroundRunNumberField.field.text()
        self.sampleIndex = view.sampleDropdown.currentIndex()
        self.prevGroupingIndex = view.groupingFileDropdown.currentIndex()
        self.samplePath = view.sampleDropdown.currentText()
        self.focusGroupPath = view.groupingFileDropdown.currentText()
        self.prevXtalDMin = float(self._tweakPeakView.fieldXtalDMin.field.text())
        self.prevXtalDMax = float(self._tweakPeakView.fieldXtalDMax.field.text())

        # Validate that the user has write permissions as early as possible in the workflow.
        permissionsRequest = CalibrationWritePermissionsRequest(
            runNumber=self.runNumber, continueFlags=self.continueAnywayFlags
        )
        self.request(path="normalization/validateWritePermissions", payload=permissionsRequest)

        # init the payload
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=self.focusGroups[self.focusGroupPath],
            crystalDBounds={"minimum": self.prevXtalDMin, "maximum": self.prevXtalDMax},
            continueFlags=self.continueAnywayFlags,
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

        self.normalizationResponse = self.request(path="normalization", payload=payload.json())
        focusWorkspace = self.normalizationResponse.data["focusedVanadium"]
        smoothWorkspace = self.normalizationResponse.data["smoothedVanadium"]
        self.correctedVanadiumWorkspace = self.normalizationResponse.data["correctedVanadium"]
        peaks = self.normalizationResponse.data["detectorPeaks"]
        self.calibrationRunNumber = self.normalizationResponse.data["calibrationRunNumber"]
        # calculate residual
        residualWorkspace = self._calcResidual(focusWorkspace, smoothWorkspace)

        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks, residualWorkspace)
        self.initializationComplete = True
        return self.normalizationResponse

    def _calcResidual(self, focusWorkspace, smoothWorkspace):
        residualReq = CalculateNormalizationResidualRequest(
            runNumber=self.runNumber, dataWorkspace=focusWorkspace, calculationWorkspace=smoothWorkspace
        )
        residualWorkspace = self.request(path="normalization/calculateResidual", payload=residualReq).data
        return residualWorkspace

    @EntryExitLogger(logger=logger)
    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _specifyNormalization(self, workflowPresenter):  # noqa: ARG002
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=str(self.samplePaths[self.sampleIndex]),
            focusGroup=list(self.focusGroups.items())[self.prevGroupingIndex][1],
            smoothingParameter=self.prevSmoothingParameter,
            crystalDBounds={"minimum": self.prevXtalDMin, "maximum": self.prevXtalDMax},
            continueFlags=self.continueAnywayFlags,
        )
        self.recordResponse = self.request(path="normalization/assessment", payload=payload.json())
        return self.recordResponse

    @EntryExitLogger(logger=logger)
    @Slot(WorkflowPresenter, result=SNAPResponse)
    def _saveNormalization(self, workflowPresenter):
        view = workflowPresenter.widget.tabView
        runNumber = view.fieldRunNumber.get()
        version = view.fieldVersion.get(VersionState.NEXT)
        appliesTo = view.fieldAppliesTo.get(f">={self.calibrationRunNumber}")
        # validate version number
        version = VersionedObject(version=version).version
        # validate appliesTo field
        appliesTo = IndexEntry.appliesToFormatChecker(appliesTo)

        normalizationRecord = self.recordResponse.data
        normalizationRecord.workspaceNames.append(self.normalizationResponse.data["smoothedVanadium"])
        normalizationRecord.workspaceNames.append(self.normalizationResponse.data["focusedVanadium"])
        normalizationRecord.workspaceNames.append(self.normalizationResponse.data["correctedVanadium"])

        createIndexEntryRequest = CreateIndexEntryRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            appliesTo=appliesTo,
            comments=view.fieldComments.get(),
            author=view.fieldAuthor.get(),
        )
        createRecordRequest = CreateNormalizationRecordRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            version=version,
            calculationParameters=normalizationRecord.calculationParameters,
            backgroundRunNumber=normalizationRecord.backgroundRunNumber,
            smoothingParameter=normalizationRecord.smoothingParameter,
            workspaceNames=normalizationRecord.workspaceNames,
            calibrationVersionUsed=normalizationRecord.calibrationVersionUsed,
            crystalDBounds=normalizationRecord.crystalDBounds,
            normalizationCalibrantSamplePath=normalizationRecord.normalizationCalibrantSamplePath,
        )

        payload = NormalizationExportRequest(
            createIndexEntryRequest=createIndexEntryRequest,
            createRecordRequest=createRecordRequest,
        )
        response = self.request(path="normalization/save", payload=payload.json())
        return response

    @EntryExitLogger(logger=logger)
    def callNormalization(self, index, smoothingParameter, xtalDMin, xtalDMax, correctedVanadiumWs=None):
        payload = NormalizationRequest(
            runNumber=self.runNumber,
            useLiteMode=self.useLiteMode,
            backgroundRunNumber=self.backgroundRunNumber,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=list(self.focusGroups.items())[index][1],
            smoothingParameter=smoothingParameter,
            crystalDBounds={"minimum": xtalDMin, "maximum": xtalDMax},
            continueFlags=self.continueAnywayFlags,
            correctedVanadiumWs=correctedVanadiumWs,
        )
        self.normalizationResponse = self.request(path="normalization", payload=payload.json())

        focusWorkspace = self.normalizationResponse.data["focusedVanadium"]
        smoothWorkspace = self.normalizationResponse.data["smoothedVanadium"]
        self.correctedVanadiumWorkspace = self.normalizationResponse.data["correctedVanadium"]
        peaks = self.normalizationResponse.data["detectorPeaks"]

        residualWorkspace = self._calcResidual(focusWorkspace, smoothWorkspace)

        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks, residualWorkspace)

    @EntryExitLogger(logger=logger)
    def applySmoothingUpdate(self, index, smoothingValue, xtalDMin, xtalDMax):
        focusWorkspace = self.normalizationResponse.data["focusedVanadium"]
        smoothWorkspace = self.normalizationResponse.data["smoothedVanadium"]

        payload = SmoothDataExcludingPeaksRequest(
            inputWorkspace=focusWorkspace,
            useLiteMode=self.useLiteMode,
            outputWorkspace=smoothWorkspace,
            calibrantSamplePath=self.samplePaths[self.sampleIndex],
            focusGroup=list(self.focusGroups.items())[index][1],
            runNumber=self.runNumber,
            smoothingParameter=smoothingValue,
            crystalDMin=xtalDMin,
            crystalDMax=xtalDMax,
        )
        response = self.request(path="normalization/smooth", payload=payload.json())

        peaks = response.data["detectorPeaks"]
        residualWorkspace = self._calcResidual(focusWorkspace, smoothWorkspace)

        self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks, residualWorkspace)

    @EntryExitLogger(logger=logger)
    @ExceptionToErrLog
    @Slot(int, float, float, float)
    def onNormalizationValueChange(self, *args):  # noqa: ARG002
        if not self.initializationComplete:
            return
        # disable recalculate button
        self._tweakPeakView.disableRecalculateButton()
        self.workflow.presenter.handleAction(
            self.renewWhenRecalculate,
            args=args,
            onSuccess=self._tweakPeakView.enableRecalculateButton,
        )

    def renewWhenRecalculate(self, index, smoothingValue, xtalDMin, xtalDMax):
        # if the grouping file change, redo whole calculation
        groupingFileChanged = index != self.prevGroupingIndex
        # if peaks will change, redo only the smoothing
        smoothingValueChanged = self.prevSmoothingParameter != smoothingValue
        xtalDMinValueChanged = xtalDMin != self.prevXtalDMin
        xtalDMaxValueChanged = xtalDMax != self.prevXtalDMax
        peakListWillChange = smoothingValueChanged or xtalDMinValueChanged or xtalDMaxValueChanged

        # check the case, apply correct update
        if groupingFileChanged:
            self.callNormalization(
                index, smoothingValue, xtalDMin, xtalDMax, correctedVanadiumWs=self.correctedVanadiumWorkspace
            )
        elif peakListWillChange:
            self.applySmoothingUpdate(index, smoothingValue, xtalDMin, xtalDMax)
        elif "focusedVanadium" in self.responses[-1].data and "smoothedVanadium" in self.responses[-1].data:
            # if nothing changed but this function was called anyway... just replot stuff with old values
            focusWorkspace = self.responses[-1].data["focusedVanadium"]
            smoothWorkspace = self.responses[-1].data["smoothedVanadium"]
            peaks = self.responses[-1].data["detectorPeaks"]
            residualWorkspace = self._calcResidual(focusWorkspace, smoothWorkspace)

            self._tweakPeakView.updateWorkspaces(focusWorkspace, smoothWorkspace, peaks, residualWorkspace)
        else:
            raise Exception("Expected data not found in the last response")

        # update the values for next call to this method
        self.prevGroupingIndex = index
        self.prevSmoothingParameter = smoothingValue
        self.prevXtalDMin = xtalDMin
        self.prevXtalDMax = xtalDMax

        return SNAPResponse(code=ResponseCode.OK)

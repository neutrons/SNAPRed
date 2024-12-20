from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from qtpy.QtCore import Qt, QTimer, Slot

from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients
from snapred.backend.dao.LiveMetadata import LiveMetadata
from snapred.backend.dao.request import (
    CreateArtificialNormalizationRequest,
    MatchRunsRequest,
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.ui.view.reduction.ArtificialNormalizationView import ArtificialNormalizationView
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer


logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionRequestView = ReductionRequestView(
            parent=parent,
            getCompatibleMasks=self._getCompatibleMasks,
            validateRunNumbers=self._validateRunNumbers,
            getLiveMetadata=self._getLiveMetadata
        )
        if not self._hasLiveDataConnection():
            # Only enable live-data mode if there is a connection to the listener.
            self._reductionRequestView.setLiveDataToggleEnabled(False)
        
        self._compatibleMasks: Dict[str, WorkspaceName] = {}

        self._artificialNormalizationView = ArtificialNormalizationView(parent=parent)

        self.workflow = (
            WorkflowBuilder(
                startLambda=self.start,
                # Retain reduction-output workspaces.
                resetLambda=lambda: self.reset(True),
                cancelLambda=self.cancelWorkflow,
                completeWorkflowLambda=self.completeWorkflow,
                parent=parent,
            )
            .addNode(
                self._triggerReduction,
                self._reductionRequestView,
                "Reduction",
                continueAnywayHandler=self._continueAnywayHandler,
            )
            .addNode(
                self._continueWithNormalization,
                self._artificialNormalizationView,
                "Artificial Normalization",
            )
            .build()
        )
        
        ##
        ## WORKFLOW-STATE attributes:
        ##
        
        # Most of these should also be updated at `self._triggerReduction`
        #   using the current settings from `ReductionRequestView`.
        
        self._keeps: Set[WorkspaceName] = set()
        self.runNumbers: List[str] = []
        self.useLiteMode: bool = True
        
        self.liveDataMode: bool = False
        self.liveDataDuration: timedelta = timedelta(seconds=0)
        # Control of live-data metadata update:
        self._liveDataUpdateTimer = QTimer()
        self.addResetHook(
            lambda: self._reductionRequestView.setLiveDataToggleEnabled(True) if not self.liveDataMode else None
        )
        
        # Control of live-data reduction workflow loop:
        self._workflowTimer = QTimer()
        
        self.pixelMasks: List[WorkspaceName] = []
        
        ##
        ## Connect signals to slots:
        ##
        
        # Start automatic update at live-data mode change:
        self._reductionRequestView.liveDataModeChange.connect(self.updateLiveMetadata)
        
        # Restart automatic update at end-of-reset following workflow completion:
        self.workflow.presenter.resetCompleted.connect(lambda: self.updateLiveMetadata(self.liveDataMode))
        
        self._artificialNormalizationView.signalValueChanged.connect(self.onArtificialNormalizationValueChange)
        
        # Note: in order to simplify the flow-of-control,
        #   all of the `ReductionRequestView` signal connections have been moved to `ReductionRequestView` itself,
        #     which is now a `QStackedOverlay` consisting of multiple sub-views. 

    def _enableConvertToUnits(self):
        state = self._reductionRequestView.retainUnfocusedDataCheckbox.isChecked()
        self._reductionRequestView.convertUnitsDropdown.setEnabled(state)

    def _nothing(self, workflowPresenter):  # noqa: ARG002
        return SNAPResponse(code=200)

    def start(self):
        self._reductionRequestView.setLiveDataToggleEnabled(False)
        super().start()

    def cancelWorkflow(self):
        # This method exists in order to correctly shut down the live-data loop.
        def _safeShutdown():
            self._reductionRequestView.setLiveDataToggleEnabled(True)
            if self._workflowTimer.isActive():
                self._workflowTimer.stop()
            self.workflow.presenter.safeShutdown()
            
        self.workflow.presenter.resetWithPermission(
            shutdownLambda=_safeShutdown
        )
    
    def completeWorkflow(self):
        if not self.liveDataMode:
            panelText = ""
            if (
                self.continueAnywayFlags is not None
                and ContinueWarning.Type.NO_WRITE_PERMISSIONS in self.continueAnywayFlags
            ):
                panelText = (
                    "<p>You didn't have permissions to write to "
                    + f"<br><b>{self.savePath}</b>,<br>"
                    + "but you can still save using the workbench tools.</p>"
                    + "<p>Please remember to save your output workspaces!</p>"
                )
            else:
                panelText = (
                    "<p>Reduction has completed successfully!"
                    + "<br>Reduction workspaces have been saved to "
                    + f"<br><b>{self.savePath}</b>.<br></p>"
                    + "<p>If required later, these can be reloaded into Mantid workbench using 'LoadNexus'.</p>"
                )
            self.workflow.presenter.completeWorkflow(message=panelText)
        else:
            # Live-data loop: exit is by cancellation only:
            
            request_: ReductionRequest = self.requests[-1]
            response: ReductionResponse = self.responses[-1]
            
            # -- `presenter.completeWorkflow` will call `reset`, which gets us back to the live-data summary panel.
            self.workflow.presenter.completeWorkflow(message=None)
            
            updateInterval = self._liveDataUpdateInterval()
            if response.executionTime > updateInterval:
                # Immediately start the next reduction.
                self.request(path="reduction/", payload=request_)
            else:
                # Wait and then start the next reduction.
                waitTime = updateInterval - response.executionTime
                self._workflowTimer.singleShot(waitTime.seconds * 1000, Qt.CoarseTimer, lambda: self.request(path="reduction/", request=request_))

    def _setInteractive(self, state: bool):
        
        # Sorry! Two proceeding underscores is reserved!

        self._reductionRequestView._setInteractive(state)
        # self._reductionRequestView.liteModeToggle.setEnabled(state)
        # self._reductionRequestView.liveDataToggle.setEnabled(state)
        # self._reductionRequestView.pixelMaskDropdown.setEnabled(state)
        # self._reductionRequestView.retainUnfocusedDataCheckbox.setEnabled(state)

    def _getCompatibleMasks(self, runNumbers: List[str], useLiteMode: bool) -> List[str]:
        # Get compatible masks for the current reduction state.
        masks = []

        if runNumbers:

            compatibleMasks = self.request(
                path="reduction/getCompatibleMasks",
                payload=ReductionRequest(
                    # All runNumbers are from the same state => any one can be used here
                    runNumber=runNumbers[0],
                    useLiteMode=useLiteMode,
                ),
            ).data

            # Map from mask-name strings to their corresponding WorkspaceName objects.
            self._compatibleMasks = {name.toString(): name for name in compatibleMasks}
            masks = list(self._compatibleMasks.keys())

        return masks

    def _liveDataUpdateInterval(self) -> timedelta:
        return self._reductionRequestView.liveDataUpdateInterval()

    @Slot(bool)
    def updateLiveMetadata(self, liveDataMode: bool):
        self.liveDataMode = liveDataMode
    
        # Start metadata update at live-data mode change, 
        #   or restart it at the completion of a reduction workflow.
        if self._liveDataUpdateTimer.isActive():
            self._liveDataUpdateTimer.stop()
        if liveDataMode:
            self._updateLiveMetadata()
        else:
            # WARNING: live-data mode can disable the continue button,
            #   so we need to re-enable it here, just in case.
            self.workflow.presenter.enableButtons(True)
    
    @Slot()
    def _updateLiveMetadata(self):
        if not self.workflow.presenter.workflowIsRunning:
            # Don't harass the data listener if it's already in a retrieval cycle!
            data = self._getLiveMetadata()
            self._reductionRequestView.updateLiveMetadata(data)
            
            # Disable buttons in case of "no active run" or "beam down" conditions.
            if not data.hasActiveRun() or not data.beamState():
                self.workflow.presenter.enableButtons(False)
        
        # Automatically update live metadata every update interval.
        updateInterval = self._liveDataUpdateInterval().seconds * 1000
        self._liveDataUpdateTimer.singleShot(updateInterval, Qt.CoarseTimer, self._updateLiveMetadata)
    
    def _hasLiveDataConnection(self) -> bool:
        return self.request(path="reduction/hasLiveDataConnection").data
    
    def _getLiveMetadata(self) -> LiveMetadata:
        return self.request(path="reduction/getLiveMetadata").data

    def _validateRunNumbers(self, runNumbers: List[str]):
        # For now, all run numbers in a reduction batch must be from the same instrument state.
        # This is primarily because pixel-mask selection occurs by instrument state.
        stateIds = []
        try:
            stateIds = self.request(path="reduction/getStateIds", payload=runNumbers).data
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Unable to get instrument state for {runNumbers}: {e}")
        if len(stateIds) > 1 and len(set(stateIds)) > 1:
            raise ValueError("All run numbers must be from the same state")

    def _reconstructPixelMaskNames(self, pixelMasks: List[str]) -> List[WorkspaceName]:
        return [self._compatibleMasks[name] for name in pixelMasks]

    def _createReductionRequest(self, runNumber, artificialNormalizationIngredients=None):
        """
        Create a standardized ReductionRequest object for passing to the ReductionService
        """
        return ReductionRequest(
            runNumber=str(runNumber),
            useLiteMode=self.useLiteMode,
            liveDataMode=self.liveDataMode,
            liveDataDuration=self.liveDataDuration,
            timestamp=self.timestamp,
            continueFlags=self.continueAnywayFlags,
            pixelMasks=self.pixelMasks,
            keepUnfocused=self._reductionRequestView.keepUnfocused(),
            convertUnitsTo=self._reductionRequestView.convertUnitsTo(),
            artificialNormalizationIngredients=artificialNormalizationIngredients,
        )

    def _triggerReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView  # noqa: F841

        self.runNumbers = self._reductionRequestView.getRunNumbers()
        self.useLiteMode = self._reductionRequestView.useLiteMode()
        self.liveDataMode = self._reductionRequestView.liveDataMode()
        self.liveDataDuration = self._reductionRequestView.liveDataDuration()
        self.pixelMasks = self._reconstructPixelMaskNames(self._reductionRequestView.getPixelMasks())

        # Use one timestamp for the entire set of runNumbers:
        self.timestamp = self.request(path="reduction/getUniqueTimestamp").data
        
        # All runs are from the same state, use the first run to load groupings.
        request_ = self._createReductionRequest(self.runNumbers[0])
        response = self.request(path="reduction/groupings", payload=request_)
        self._keeps = set(response.data["groupingWorkspaces"])

        # Get the calibration and normalization versions for all runs to be processed
        matchRequest = MatchRunsRequest(runNumbers=self.runNumbers, useLiteMode=self.useLiteMode)
        loadedCalibrations, calVersions = self.request(path="calibration/fetchMatches", payload=matchRequest).data
        loadedNormalizations, normVersions = self.request(path="normalization/fetchMatches", payload=matchRequest).data
        self._keeps.update(loadedCalibrations)
        self._keeps.update(loadedNormalizations)

        distinctNormVersions = set(normVersions.values())
        if len(distinctNormVersions) > 1 and None in distinctNormVersions:
            raise RuntimeError(
                "Some of your workspaces require Artificial Normalization.  "
                "SNAPRed can currently only handle the situation where all, or none "
                "of the runs require Artificial Normalization.  Please clear the list "
                "and try again."
            )

        # Validate reduction; if artificial normalization is needed, handle it
        # NOTE: this logic ONLY works because we are forbidding mixed cases of artnorm or loaded norm
        response = self.request(path="reduction/validate", payload=request_)
        if ContinueWarning.Type.MISSING_NORMALIZATION in self.continueAnywayFlags:
            if len(self.runNumbers) > 1:
                raise RuntimeError(
                    "Currently, Artificial Normalization can only be performed on a "
                    "single run at a time.  Please clear your run list and try again."
                )
            for runNumber in self.runNumbers:
                self._artificialNormalizationView.updateRunNumber(runNumber)
                self._artificialNormalizationView.showAdjustView()
                request_ = self._createReductionRequest(runNumber)
                response = self.request(path="reduction/grabWorkspaceforArtificialNorm", payload=request_)
                self._artificialNormalization(workflowPresenter, response.data, runNumber)
        else:
            for runNumber in self.runNumbers:
                self._artificialNormalizationView.showSkippedView()
                request_ = self._createReductionRequest(runNumber)
                response = self.request(path="reduction/", payload=request_)
                if response.code == ResponseCode.OK:
                    self._finalizeReduction(response.data.record, response.data.unfocusedData)
                
                # after each run, clean workspaces except groupings, calibrations, normalizations, and outputs
                self._keeps.update(self.outputs)
                self._clearWorkspaces(exclude=self._keeps, clearCachedWorkspaces=True)
            
            workflowPresenter.advanceWorkflow()
        
        # SPECIAL FOR THE REDUCTION WORKFLOW: clear everything _except_ the output workspaces
        #   _before_ transitioning to the "save" panel.
        # TODO: make '_clearWorkspaces' a public method (i.e make this combination a special `cleanup` method).
        self._clearWorkspaces(exclude=self.outputs, clearCachedWorkspaces=True)
        
        return self.responses[-1]

    def _artificialNormalization(self, workflowPresenter, responseData, runNumber):
        """Handles artificial normalization for the workflow."""
        view = workflowPresenter.widget.tabView  # noqa: F841
        request_ = CreateArtificialNormalizationRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            peakWindowClippingSize=int(self._artificialNormalizationView.peakWindowClippingSize.field.text()),
            smoothingParameter=self._artificialNormalizationView.getSmoothingParameter(),
            decreaseParameter=self._artificialNormalizationView.decreaseParameterDropdown.currentIndex() == 1,
            lss=self._artificialNormalizationView.lssDropdown.currentIndex() == 1,
            diffractionWorkspace=responseData,
        )
        response = self.request(path="reduction/artificialNormalization", payload=request_)
        # Update artificial normalization view with the response
        if response.code == ResponseCode.OK:
            self._artificialNormalizationView.updateWorkspaces(responseData, response.data)
        else:
            raise RuntimeError("Failed to run artificial normalization.")

        return self.responses[-1]

    @Slot(float, bool, bool, int)
    def onArtificialNormalizationValueChange(self, smoothingValue, lss, decreaseParameter, peakWindowClippingSize):
        """Updates artificial normalization based on user input."""
        self._artificialNormalizationView.disableRecalculateButton()
        runNumber = self._artificialNormalizationView.fieldRunNumber.text()
        diffractionWorkspace = self._artificialNormalizationView.diffractionWorkspace

        request_ = CreateArtificialNormalizationRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            peakWindowClippingSize=peakWindowClippingSize,
            smoothingParameter=smoothingValue,
            decreaseParameter=decreaseParameter,
            lss=lss,
            diffractionWorkspace=diffractionWorkspace,
        )

        response = self.request(path="reduction/artificialNormalization", payload=request_)
        
        #
        # TODO: why isn't this checking that the request actually succeeded?
        #   More significantly, why isn't this entire method just a call to `self._artificialNormalization`?
        #   (Or, if that's an issue, `self._artificialNormalization` should just include a call to this method.)
        #
        
        self._artificialNormalizationView.updateWorkspaces(diffractionWorkspace, response.data)
        self._artificialNormalizationView.enableRecalculateButton()

    def _continueWithNormalization(self, workflowPresenter):  # noqa: ARG002
        """Continues the workflow using the artificial normalization workspace."""
        
        artificialNormIngredients = ArtificialNormalizationIngredients(
            peakWindowClippingSize=self._artificialNormalizationView.getPeakWindowClippingSize(),
            smoothingParameter=self._artificialNormalizationView.getSmoothingParameter(),
            decreaseParameter=self._artificialNormalizationView.getDecreaseParameter(),
            lss=self._artificialNormalizationView.getLSS(),
        )

        # Here we use the standardized `_createReductionRequest` method.
        # We do NOT set a new timestamp,
        #   nor do we re-initialize any other values "by hand" that may have nothing to do
        #   with this artificial normalization step.
        request_ = self._createReductionRequest(
            runNumber=self._artificialNormalizationView.fieldRunNumber.text(),
            artificialNormalizationIngredients=artificialNormIngredients
        ) 

        response = self.request(path="reduction/", payload=request_)
        if response.code == ResponseCode.OK:
            record, unfocusedData = response.data.record, response.data.unfocusedData
            self._finalizeReduction(record, unfocusedData)

        return self.responses[-1]

    def _finalizeReduction(self, record, unfocusedData):
        """Handles post-reduction tasks, including saving and workspace management."""
        self.savePath = self.request(path="reduction/getSavePath", payload=record.runNumber).data
        # Save the reduced data. (This is automatic: it happens before the "save" panel opens.)
        if ContinueWarning.Type.NO_WRITE_PERMISSIONS not in self.continueAnywayFlags:
            self.request(path="reduction/save", payload=ReductionExportRequest(record=record))
            # Retain the output workspaces after the workflow is complete.
        self.outputs.extend(record.workspaceNames)
        # Also retain the unfocused data after the workflow is complete (if the box was checked),
        #   but do not actually save it as part of the reduction-data file.
        # The unfocused data does not get added to the response.workspaces list.
        if unfocusedData:
            self.outputs.append(unfocusedData)
            # Note that the run number is deliberately not deleted from the run numbers list.
            # Almost certainly it should be moved to a "completed run numbers" list.

    @property
    def widget(self):
        return self.workflow.presenter.widget

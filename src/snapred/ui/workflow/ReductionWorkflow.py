from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from qtpy.QtCore import QMetaObject, Qt, QTimer, Signal, Slot

from snapred.backend.dao.ingredients import ArtificialNormalizationIngredients
from snapred.backend.dao.request import (
    CreateArtificialNormalizationRequest,
    MatchRunsRequest,
    ReductionExportRequest,
    ReductionRequest,
    RunMetadataRequest,
)
from snapred.backend.dao.response.ReductionResponse import ReductionResponse
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter
from snapred.ui.view.reduction.ArtificialNormalizationView import ArtificialNormalizationView
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionStatus(StrEnum):
    """
    Implementation notes:

    This is just a "first pass" on a workflow-status enum:

    -- This could also have been a _part_ of a more generalized workflow state.

    -- This enum class may or may not generalize _across_ workflows; in this initial design
    it is assumed that it does not.
    """

    READY = "Ready"
    CALCULATING_NORM = "Calculating normalization"
    REDUCING_DATA = "Reducing data"
    FINALIZING = "Finalizing reduction"
    USER_CANCELLATION = "Cancelling workflow"

    # Special to live data:
    CONNECTING = "Connecting to listener"
    WAITING_TO_LOAD = "Waiting to load next chunk"
    NO_ACTIVE_RUN = "No run is active"
    ZERO_PROTON_CHARGE = "Proton charge is zero"


class ReductionWorkflow(WorkflowImplementer):
    _liveMetadataUpdate = Signal(RunMetadata)
    _statusUpdate = Signal(ReductionStatus)

    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionRequestView = ReductionRequestView(
            parent=parent,
            getCompatibleMasks=self._getCompatibleMasks,
            validateRunNumbers=self._validateRunNumbers,
            getLiveMetadata=self._getLiveMetadata,
        )

        self._reductionRequestView._requestView.setRunMetadataCallback(self.handleRunMetadata)

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

        self.setStatus(ReductionStatus.READY)

        # Quite a few of these attributes are also set at `self._triggerReduction`
        #   using the current settings from `ReductionRequestView`.

        self._keeps: Set[WorkspaceName] = set()
        self.runNumbers: List[str] = []
        self.useLiteMode: bool = True

        self.liveDataMode: bool = False
        self.liveDataDuration: timedelta = timedelta(seconds=0)

        # Initialize a timer to control the live-data metadata update.
        self._liveDataUpdateTimer = QTimer(self)
        #   A "chain" update is used here, to prevent runaway-timer issues.
        self._liveDataUpdateTimer.setSingleShot(True)
        self._liveDataUpdateTimer.setTimerType(Qt.CoarseTimer)
        self._liveDataUpdateTimer.setInterval(Config["liveData.updateIntervalDefault"] * 1000)
        self._liveDataUpdateTimer.timeout.connect(self.updateLiveMetadata)

        # Only enable live-data mode if there is a connection to the listener.
        self._reductionRequestView.setLiveDataToggleEnabled(self._hasLiveDataConnection())
        self.workflow.presenter.resetCompleted.connect(
            # Notes:
            # -- When _not_ in live-data mode, this controls the live-data toggle enable;
            # -- When in live-data mode, the toggle is enabled when the workflow is not cycling;
            # -- This hook overrides the normal reset behavior of re-enabling all of the toggles,
            #    for this reason it must be executed _after_ all of the other reset hooks!
            lambda: (
                self._reductionRequestView.setLiveDataToggleEnabled(self._hasLiveDataConnection())
                if not self.liveDataMode
                else None
            )
        )

        # Initialize a separate timer for the control of the live-data reduction-workflow loop.
        self._workflowTimer = QTimer(self)
        self._workflowTimer.setSingleShot(True)
        self._workflowTimer.setTimerType(Qt.CoarseTimer)
        self._workflowTimer.setInterval(Config["liveData.updateIntervalDefault"] * 1000)
        self._lastReductionRequest = None  # used by `_reduceLiveDataChunk`
        self._lastReductionResponse = None  # used by `_reduceLiveDataChunk`
        self._workflowTimer.timeout.connect(self._reduceLiveDataChunk)

        self.pixelMasks: List[WorkspaceName] = []

        ##
        ## Connect signals to slots:
        ##

        self._reductionRequestView.liveDataModeChange.connect(self.setLiveDataMode)

        # `_getLiveMetadata` updates the live-data view:
        self._liveMetadataUpdate.connect(self._updateLiveMetadata)

        # `setStatus` updates the live-data view:
        self._statusUpdate.connect(self._reductionRequestView.updateStatus)

        # Presenter signals the live-data view that a reduction is in progress:
        self.workflow.presenter.workflowInProgressChange.connect(self._reductionRequestView.setReductionInProgress)

        # Status summary shows "CANCELLING WORKFLOW" on any cancellation request:
        self.workflow.presenter.cancellationRequest.connect(lambda: self.setStatus(ReductionStatus.USER_CANCELLATION))

        # Status summary is updated to "READY" at the completion of reset:
        self.workflow.presenter.resetCompleted.connect(lambda: self.setStatus(ReductionStatus.READY))

        self._artificialNormalizationView.signalValueChanged.connect(self.onArtificialNormalizationValueChange)

        # Note: in order to simplify the flow-of-control,
        #   all of the `ReductionRequestView` signal connections have been moved to `ReductionRequestView` itself,
        #     which is now a `QStackedOverlay` consisting of multiple sub-views.

    @property
    def status(self) -> ReductionStatus:
        return self._status

    def setStatus(self, status: ReductionStatus):
        # Following `Qt` style, this is _not_ @<property>.setter!
        self._status = status
        self._statusUpdate.emit(status)

    def handleRunMetadata(self, runNumber: str) -> RunMetadata:
        payload = RunMetadataRequest(runId=runNumber)
        metadata = self.request(path="calibration/runMetadata", payload=payload.json()).data
        return metadata

    def _nothing(self, workflowPresenter: WorkflowPresenter):  # noqa: ARG002
        return SNAPResponse(code=200)

    def start(self):
        self._reductionRequestView.setInteractive(False)
        super().start()

    @Slot()
    def cancelWorkflow(self):
        # This method exists in order to correctly shut down the live-data loop.
        def _safeShutdown():
            if self._workflowTimer.isActive():
                self._workflowTimer.stop()
            self._reductionRequestView.setInteractive(True)
            self.workflow.presenter.safeShutdown()

        self.workflow.presenter.resetWithPermission(shutdownLambda=_safeShutdown)

    @Slot()
    def completeWorkflow(self):
        if not self.liveDataMode:
            panelText = ""
            if (
                self.continueAnywayFlags is not None
                and ContinueWarning.Type.NO_WRITE_PERMISSIONS in self.continueAnywayFlags
            ):
                if self.savePath is not None:
                    panelText = (
                        "<p>You didn't have permissions to write to "
                        + f"<br><b>{self.savePath}</b>,<br>"
                        + "but you can still save using the workbench tools.</p>"
                        + "<p>Please remember to save your output workspaces!</p>"
                    )
                else:
                    panelText = (
                        "<p>No IPTS directory existed yet for the reduced run,<br>"
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
            # Prepare and submit the next live-data reduction request.
            self._cycleLiveData(True)

    @Slot(bool)
    def _cycleLiveData(self, success: bool):
        # Prepare the next live-data reduction request, and start its submission timer.
        cycleStatus = success
        if cycleStatus:
            try:
                # Retain the last completed `ReductionRequest`,
                #   and its `ReductionResponse` before any `reset` is called.

                # TODO: I'd prefer if `self._lastReductionRequest` and `self._lastReductionResponse`
                #   were local variables, which could then be passed to the submitted reduction-service
                #   'reduction' for each live-data cycle.
                # However, in order to use non-static `QTimer` methods,
                #   the slot target of the `_workflowTimer.timeout` needs to be fixed at `__init__`,
                #   so for the moment these are passed as attributes of the workflow.

                # Right now, the <normalization> and <artificial normalization> reduction paths
                #   just happen to have the same last-request index.  If this changes,
                #   then setting of `self._lastReductionRequest` and `self._lastReductionResponse`
                #   must take that into account.

                lastRequestIndex = -2
                self._lastReductionRequest: ReductionRequest = self.requests[lastRequestIndex].payload
                self._lastReductionResponse: ReductionResponse = self.responses[lastRequestIndex].data

                # Calling `presenter.resetSoft()` gets us back to the live-data summary panel.
                self.workflow.presenter.resetSoft()

                # Since they are replaced only at the end of the reduction process,
                #   and the first reduction cycle has been completed and has produced valid outputs:
                #   do _not_ delete output workspaces in case of error.
                self.outputs.update(self._lastReductionResponse.record.workspaceNames)

                updateInterval = self._liveDataUpdateInterval()

                # If the user has set the updateInterval to too small a value, we just do the best we can.
                # (We do _not_ screw up non-interactivity by spamming the logs with a WARNING!)

                waitTime: timedelta = updateInterval - self._lastReductionResponse.executionTime
                if waitTime < timedelta(seconds=0):
                    waitTime = timedelta(seconds=0)

                self.setStatus(ReductionStatus.WAITING_TO_LOAD)

                # Submit the reduction request for the next live-data chunk:
                #   the slot target of the `_workflowTimer.timeout` has been set to
                #  `_reduceLiveDataChunk` during `__init__`.
                self._workflowTimer.setInterval(waitTime.seconds * 1000)
                self._workflowTimer.start()
            except (AttributeError, IndexError) as e:
                logger.error(f"_cycleLiveData: unexpected: {e}")
                cycleStatus = False
        if not cycleStatus:
            if self.status != ReductionStatus.READY:
                # "READY" indicates that this `reset` would be redundant.
                # (e.g. User cancellation has its own `reset` pathway.)
                self.workflow.presenter.reset()

    @Slot()
    def _reduceLiveDataChunk(self):
        # Submit a live-data reduction request.

        def _reduceLiveData():
            self.setStatus(ReductionStatus.REDUCING_DATA)

            response = self.request(path="reduction/", payload=self._lastReductionRequest)
            if response.code == ResponseCode.OK:
                # Finalize the reduction.
                record, unfocusedData = response.data.record, response.data.unfocusedData
                self._finalizeReduction(record, unfocusedData)

            # after each cycle, clean workspaces except groupings, calibrations, normalizations, and outputs
            self._keeps.update(self.outputs)
            self._clearWorkspaces(exclude=self._keeps, clearCachedWorkspaces=True)

            return self.responses[-1]

        # Resubmit the previous request, which will then act on the next live-data chunk.
        self._submitActionToPresenter(_reduceLiveData, None, self._cycleLiveData)

    def _submitActionToPresenter(
        self,
        action: Callable[[Any], Any],
        args: Tuple[Any, ...] | Any | None = None,
        onSuccess: Callable[[bool], None] = lambda flag: None,  # noqa: ARG005
        isWorkflow: bool = True,
    ):
        # Submit an action to this workflow's presenter's thread pool.
        self.workflow.presenter.handleAction(action, args, onSuccess, isWorkflow=isWorkflow)

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
    def setLiveDataMode(self, flag: bool):
        self.liveDataMode = flag
        if self._liveDataUpdateTimer.isActive():
            self._liveDataUpdateTimer.stop()
        if self.liveDataMode:
            # After the first continue click from the request panel,
            #   and from the artificial-normalization panel, if it's used,
            #   workflow nodes in live-data mode are automatically continued.
            self.workflow.presenter.setManualMode(False)

            # Start the metadata update sequence.
            self.updateLiveMetadata(True)
        else:
            self.workflow.presenter.setManualMode(True)

            # Live-data mode can disable the continue button,
            #   so we need to re-enable it here, just in case.
            self.workflow.presenter.enableButtons(True)

    @Slot()
    def updateLiveMetadata(self, startup: bool = False):
        # This method continues the live-metadata timer chain.
        # If `startup` is True, then the `reductionRequestView` will be re-initialized.

        #   The actual update of the live-metadata view occurs
        #   via the `metadataUpdate` signal, emitted by `_getLiveMetadata`, triggering the `_updateLiveMetadata` slot.
        if self.liveDataMode:
            if startup:
                # After any live-data mode change,
                #   the `reductionRequestView` always starts with the "connecting to listener..." message,
                #   so that it won't display _stale_ metadata.
                self._reductionRequestView.updateLiveMetadata(None)
                self._reductionRequestView.updateStatus(self.status)

            # Don't additionally harass the data listener for metadata update
            #   while any workflow-related action is in process.
            if not self.workflow.presenter.workflowIsRunning:
                self._submitActionToPresenter(self._getLiveMetadata, None, isWorkflow=False)

            # Continue the automatic metadata update sequence.
            self._liveDataUpdateTimer.start()

    @Slot(object)  # Signal(Optional[RunMetadata]) as Signal(object)
    def _updateLiveMetadata(self, data: Optional[RunMetadata]):
        self._reductionRequestView.updateLiveMetadata(data)

    def _hasLiveDataConnection(self) -> bool:
        return self.request(path="reduction/hasLiveDataConnection").data

    def _getLiveMetadata(self) -> SNAPResponse:
        # This method defines an action so that the live metadata can be retrieved in a background thread.
        response = self.request(path="reduction/getLiveMetadata")
        if response.code == ResponseCode.OK:
            self._liveMetadataUpdate.emit(response.data)
        return response

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

    def _triggerReduction(self, workflowPresenter: WorkflowPresenter):
        view = workflowPresenter.widget.tabView  # noqa: F841

        self.runNumbers = self._reductionRequestView.getRunNumbers()
        self.useLiteMode = self._reductionRequestView.useLiteMode()
        self.liveDataMode = self._reductionRequestView.liveDataMode()
        self.liveDataDuration = self._reductionRequestView.liveDataDuration()
        self.pixelMasks = self._reconstructPixelMaskNames(self._reductionRequestView.getPixelMasks())

        # Set status to indicate the normal reduction workflow.
        # (This will be overridden in several cases, including the use of artificial normalization.)
        self.setStatus(ReductionStatus.REDUCING_DATA)

        # Use one timestamp for the entire set of runNumbers:
        self.timestamp = self.request(path="reduction/getUniqueTimestamp").data

        # All runs are from the same state, use the first run to load groupings.
        request_ = self._createReductionRequest(self.runNumbers[0])
        response = self.request(path="reduction/groupings", payload=request_)

        # Set of workspaces to retain is INITIALIZED here: after this point, we add to the set.
        self._keeps = set(response.data["groupingWorkspaces"])

        # Reload the lite-grouping-map only when necessary.
        self._keeps.add(wng.liteDataMap().build())

        # Validate reduction; if artificial normalization is needed, handle it
        # NOTE: this logic ONLY works because we are forbidding mixed cases of artnorm or loaded norm
        response = self.request(path="reduction/validate", payload=request_)

        # Get the calibration and normalization versions for all runs to be processed
        matchRequest = MatchRunsRequest(runNumbers=self.runNumbers, useLiteMode=self.useLiteMode)
        # TODO: Remove this orchestration, this should be handled in the backend
        loadedNormalizations, normVersions = self.request(path="normalization/fetchMatches", payload=matchRequest).data

        # Add loaded calibrations, calibration-masks, and normalizations to the list of workspaces to retain.
        # self._keeps.update(loadedCalibrations)
        self._keeps.update(loadedNormalizations)
        # NOTE: Normalization Workspaces are expensive to load and thus cached between reductions.
        #       This reduces the number of loads especially for the case of multiple similar runs.
        self.outputs.update(loadedNormalizations)

        distinctNormVersions = set(normVersions.values())
        if len(distinctNormVersions) > 1 and None in distinctNormVersions:
            raise RuntimeError(
                "Some of your workspaces require Artificial Normalization.  "
                "SNAPRed can currently only handle the situation where all, or none "
                "of the runs require Artificial Normalization.  Please clear the list "
                "and try again."
            )

        if ContinueWarning.Type.MISSING_NORMALIZATION in self.continueAnywayFlags and (
            ContinueWarning.Type.CONTINUE_WITHOUT_NORMALIZATION not in self.continueAnywayFlags
        ):
            if len(self.runNumbers) > 1:
                raise RuntimeError(
                    "Currently, Artificial Normalization can only be performed on a "
                    "single run at a time.  Please clear your run list and try again."
                )
            for runNumber in self.runNumbers:
                # Set status to indicate that the artificial normalization is being calculated.
                self.setStatus(ReductionStatus.CALCULATING_NORM)

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

                # after each run, clean workspaces except groupings, calibrations, and outputs
                self._keeps.update(self.outputs)
                self._clearWorkspaces(exclude=self._keeps, clearCachedWorkspaces=True)

            # Here we are probably not on the GUI thread, so we cannot call `advanceWorkflow` directly.
            QMetaObject.invokeMethod(workflowPresenter, "advanceWorkflow", Qt.QueuedConnection)

        return self.responses[-1]

    def _artificialNormalization(self, workflowPresenter, responseData, runNumber):
        """Handles artificial normalization for the workflow."""
        view = workflowPresenter.widget.tabView  # noqa: F841

        # Set status to indicate that the artificial normalization is being calculated.
        self.setStatus(ReductionStatus.CALCULATING_NORM)

        request_ = CreateArtificialNormalizationRequest(
            runNumber=runNumber,
            useLiteMode=self.useLiteMode,
            peakWindowClippingSize=int(self._artificialNormalizationView.peakWindowClippingSize.field.text()),
            smoothingParameter=self._artificialNormalizationView.getSmoothingParameter(),
            decreaseParameter=self._artificialNormalizationView.decreaseParameterDropdown.getValue(),
            lss=self._artificialNormalizationView.lssDropdown.getValue(),
            diffractionWorkspace=responseData,
            outputWorkspace=wng.artificialNormalizationPreview().runNumber(runNumber).group(wng.Groups.COLUMN).build(),
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
            outputWorkspace=wng.artificialNormalizationPreview().runNumber(runNumber).group(wng.Groups.COLUMN).build(),
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

        # Indicate that we're back to the normal reduction workflow.
        self.setStatus(ReductionStatus.REDUCING_DATA)

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
            artificialNormalizationIngredients=artificialNormIngredients,
        )
        response = self.request(path="reduction/", payload=request_)

        if response.code == ResponseCode.OK:
            record, unfocusedData = response.data.record, response.data.unfocusedData
            self._finalizeReduction(record, unfocusedData)

        # In addition to clean up, this next `_clearWorkspaces` step symmetrizes the requests / responses queue between
        #   the <has normalization> and <artificial normalization> cases.  So, if you need to remove it,
        #   please adjust the `_cycleLiveData` `lastReductionRequest` and `lastReductionResponse` accordingly!

        # After each run, clean workspaces except groupings, calibrations, normalizations, and outputs
        self._keeps.update(self.outputs)
        self._clearWorkspaces(exclude=self._keeps, clearCachedWorkspaces=True)

        return self.responses[-1]

    def _finalizeReduction(self, record, unfocusedData):
        """Handles post-reduction tasks, including saving and workspace management."""

        self.setStatus(ReductionStatus.FINALIZING)

        # Save the reduced data. (This is automatic: it happens before the "save" panel opens.)
        if not self.liveDataMode:
            self.savePath = self.request(path="reduction/getSavePath", payload=record.runNumber).data
            if ContinueWarning.Type.NO_WRITE_PERMISSIONS not in self.continueAnywayFlags:
                self.request(path="reduction/save", payload=ReductionExportRequest(record=record))

        # Retain the output workspaces after the workflow is complete.
        self.outputs.update(record.workspaceNames)

        # Also retain the unfocused data after the workflow is complete (if the box was checked),
        #   but do not actually save it as part of the reduction-data file.
        # The unfocused data does not get added to the response.workspaces list.
        if unfocusedData:
            self.outputs.add(unfocusedData)
            # Note that the run number is deliberately not deleted from the run numbers list.
            # Almost certainly it should be moved to a "completed run numbers" list.

    @property
    def widget(self):
        return self.workflow.presenter.widget

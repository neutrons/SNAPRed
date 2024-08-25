from typing import List

from snapred.backend.dao.request import (
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.view.reduction.ReductionSaveView import ReductionSaveView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionRequestView = ReductionRequestView(parent=parent, validateRunNumbers=self._validateRunNumbers)

        self._reductionSaveView = ReductionSaveView(
            parent=parent,
        )

        self.workflow = (
            WorkflowBuilder(
                startLambda=self.start,
                # Retain reduction-output workspaces.
                resetLambda=lambda: self.reset(True),
                parent=parent,
            )
            .addNode(
                self._triggerReduction,
                self._reductionRequestView,
                "Reduction",
                continueAnywayHandler=self._continueAnywayHandler,
            )
            .addNode(self._nothing, self._reductionSaveView, "Save")
            .build()
        )

    def _nothing(self, workflowPresenter):  # noqa: ARG002
        return SNAPResponse(code=200)

    def _validateRunNumbers(self, runNumbers: List[str]):
        # For now, all run numbers in a reduction batch must be from the same instrument state.
        # This is primarily because pixel-mask selection occurs by instrument state.
        stateIds = []
        try:
            stateIds = self.request(path="reduction/getStateIds", payload=runNumbers).data
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Unable to get instrument state for {runNumbers}: {e}")
        if len(stateIds) > 1:
            stateId = stateIds[0]
            for id_ in stateIds[1:]:
                if id_ != stateId:
                    raise ValueError("all run numbers must be from the same state")

    def _triggerReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView  # noqa: F841

        runNumbers = self._reductionRequestView.getRunNumbers()

        # Use one timestamp for the entire set of runNumbers:
        timestamp = self.request(path="reduction/getUniqueTimestamp").data
        for runNumber in runNumbers:
            request_ = ReductionRequest(
                runNumber=str(runNumber),
                useLiteMode=self._reductionRequestView.liteModeToggle.field.getState(),
                timestamp=timestamp,
                continueFlags=self.continueAnywayFlags,
            )

            response = self.request(path="reduction/", payload=request_)
            if response.code == ResponseCode.OK:
                record = response.data.record

                # .. update "save" panel message:
                savePath = self.request(path="reduction/getSavePath", payload=record.runNumber).data
                self._reductionSaveView.updateContinueAnyway(self.continueAnywayFlags)
                #    Warning: 'updateSavePath' uses the current 'continueAnywayFlags'
                self._reductionSaveView.updateSavePath(savePath)

                # Save the reduced data. (This is automatic: it happens before the "save" panel opens.)
                if ContinueWarning.Type.NO_WRITE_PERMISSIONS not in self.continueAnywayFlags:
                    self.request(path="reduction/save", payload=ReductionExportRequest(record=record))

                # Retain the output workspaces after the workflow is complete.
                self.outputs.extend(record.workspaceNames)

            # Note that the run number is deliberately not deleted from the run numbers list.
            # Almost certainly it should be moved to a "completed run numbers" list.

        # SPECIAL FOR THE REDUCTION WORKFLOW: clear everything _except_ the output workspaces
        #   _before_ transitioning to the "save" panel.
        # TODO: make '_clearWorkspaces' a public method (i.e make this combination a special `cleanup` method).
        self._clearWorkspaces(exclude=self.outputs, clearCachedWorkspaces=True)

        return self.responses[-1]

    @property
    def widget(self):
        return self.workflow.presenter.widget

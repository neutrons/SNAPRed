from typing import Dict, List

from qtpy.QtCore import Slot

from snapred.backend.dao.request import (
    ReductionExportRequest,
    ReductionRequest,
)
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.ui.view.reduction.ReductionRequestView import ReductionRequestView
from snapred.ui.view.reduction.ReductionSaveView import ReductionSaveView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder
from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer

logger = snapredLogger.getLogger(__name__)


class ReductionWorkflow(WorkflowImplementer):
    def __init__(self, parent=None):
        super().__init__(parent)

        self._reductionRequestView = ReductionRequestView(
            parent=parent,
            populatePixelMaskDropdown=self._populatePixelMaskDropdown,
            validateRunNumbers=self._validateRunNumbers,
        )
        self._compatibleMasks: Dict[str, WorkspaceName] = {}

        self._reductionRequestView.enterRunNumberButton.clicked.connect(lambda: self._populatePixelMaskDropdown())
        self._reductionRequestView.pixelMaskDropdown.dropDown.view().pressed.connect(self._onPixelMaskSelection)

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

        self._reductionRequestView.retainUnfocusedDataCheckbox.checkedChanged.connect(self._enableConvertToUnits)

    def _enableConvertToUnits(self):
        state = self._reductionRequestView.retainUnfocusedDataCheckbox.isChecked()
        self._reductionRequestView.convertUnitsDropdown.setEnabled(state)

    def _nothing(self, workflowPresenter):  # noqa: ARG002
        return SNAPResponse(code=200)

    @ExceptionToErrLog
    def _populatePixelMaskDropdown(self):
        if len(self._reductionRequestView.getRunNumbers()) == 0:
            return

        runNumbers = self._reductionRequestView.getRunNumbers()
        useLiteMode = self._reductionRequestView.liteModeToggle.field.getState()  # noqa: F841

        self._reductionRequestView.liteModeToggle.setEnabled(False)
        self._reductionRequestView.pixelMaskDropdown.setEnabled(False)
        self._reductionRequestView.retainUnfocusedDataCheckbox.setEnabled(False)

        # Assemble the list of compatible masks for the current reduction state --
        #   note that all run numbers should be from the same state.
        compatibleMasks = []
        try:
            compatibleMasks = self.request(
                path="reduction/getCompatibleMasks",
                payload=ReductionRequest(
                    # All runNumbers are from the same state => any one can be used here
                    runNumber=runNumbers[0],
                    useLiteMode=useLiteMode,
                ),
            ).data
        except Exception as e:  # noqa: BLE001
            print(e)

        # Create a mapping back to the original `WorkspaceName`
        #  for reconstruction of the complete type after passing through Qt.
        self._compatibleMasks = {name.toString(): name for name in compatibleMasks}

        self._reductionRequestView.pixelMaskDropdown.setItems(list(self._compatibleMasks.keys()))

        self._reductionRequestView.liteModeToggle.setEnabled(True)
        self._reductionRequestView.pixelMaskDropdown.setEnabled(True)
        self._reductionRequestView.retainUnfocusedDataCheckbox.setEnabled(True)
        # self._reductionRequestView.convertUnitsDropdown.setEnabled(True)

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

    def _reconstructPixelMaskNames(self, pixelMasks: List[str]) -> List[WorkspaceName]:
        return [self._compatibleMasks[name] for name in pixelMasks]

    @Slot()
    def _onPixelMaskSelection(self):
        pass
        #  The previous version of this method actually does nothing:  :(
        #
        # selectedKeys = self._reductionRequestView.getPixelMasks()
        # selectedWorkspaceNames = self._reconstructPixelMaskNames(selectedKeys)
        # ReductionRequest.pixelMasks = selectedWorkspaceNames
        #
        # ## Why would I want to set the ~obfuscated-by-pydantic~ `pixelMasks` field of the class object?

    def _triggerReduction(self, workflowPresenter):
        view = workflowPresenter.widget.tabView  # noqa: F841

        runNumbers = self._reductionRequestView.getRunNumbers()
        pixelMasks = self._reconstructPixelMaskNames(self._reductionRequestView.getPixelMasks())

        # Use one timestamp for the entire set of runNumbers:
        timestamp = self.request(path="reduction/getUniqueTimestamp").data
        for runNumber in runNumbers:
            request_ = ReductionRequest(
                runNumber=str(runNumber),
                useLiteMode=self._reductionRequestView.liteModeToggle.field.getState(),
                timestamp=timestamp,
                continueFlags=self.continueAnywayFlags,
                pixelMasks=pixelMasks,
                keepUnfocused=self._reductionRequestView.retainUnfocusedDataCheckbox.isChecked(),
                convertUnitsTo=self._reductionRequestView.convertUnitsDropdown.currentText(),
            )

            response = self.request(path="reduction/", payload=request_)
            if response.code == ResponseCode.OK:
                record, unfocusedData = response.data.record, response.data.unfocusedData

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

                # Also retain the unfocused data after the workflow is complete (if the box was checked),
                #   but do not actually save it as part of the reduction-data file.
                # The unfocused data does not get added to the response.workspaces list.
                if unfocusedData is not None:
                    self.outputs.append(unfocusedData)

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

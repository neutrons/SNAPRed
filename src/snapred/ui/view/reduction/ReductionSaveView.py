from pathlib import Path

from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QLabel

from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class ReductionSaveView(BackendRequestView):
    signalContinueAnyway = Signal(ContinueWarning.Type)
    signalSavePath = Signal(Path)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.continueAnywayFlags = None
        self.signalContinueAnyway.connect(self._updateContinueAnyway)

        self.savePath = None
        self.signalSavePath.connect(self._updateSavePath)

        self.saveMessage = QLabel("Please use available Workbench tools to save your workspaces before proceeding.")
        self.layout.addWidget(self.saveMessage)

    def updateContinueAnyway(self, continueAnywayFlags: ContinueWarning.Type):
        self.signalContinueAnyway.emit(continueAnywayFlags)

    @Slot(ContinueWarning.Type)
    def _updateContinueAnyway(self, continueAnywayFlags: ContinueWarning.Type):
        self.continueAnywayFlags = continueAnywayFlags

    def updateSavePath(self, path: Path):
        self.signalSavePath.emit(path)

    @Slot(Path)
    def _updateSavePath(self, path: Path):
        self.saveMessage.setEnabled(False)
        self.savePath = path
        panelText = None
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
                "<p>Reduction workspaces have been saved to "
                + f"<br><b>{self.savePath}</b>.<br></p>"
                + "<p>If required later, these can be reloaded into Mantid workbench using 'LoadNexus'.</p>"
            )
        self.saveMessage.setText(panelText)
        self.saveMessage.setEnabled(True)
        # self.saveMessage.update() # TODO: is this the correct way to do this?

    def verify(self):
        return True

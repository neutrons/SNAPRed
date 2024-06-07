from qtpy.QtWidgets import QLabel
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class ReductionSaveView(BackendRequestView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.saveMessage = QLabel(
            "Please use available Workbench tools to save your data before proceeding."
            + "\nThey are denoted with the prefix `output_"
        )
        self.layout.addWidget(self.saveMessage)

    def verify(self):
        return True

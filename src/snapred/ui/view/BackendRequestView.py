from qtpy.QtWidgets import QGridLayout, QLineEdit, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.MultiSelectDropDown import MultiSelectDropDown
from snapred.ui.widget.SampleDropDown import SampleDropDown


class BackendRequestView(QWidget):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, parent=None):
        super(BackendRequestView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

    def _labeledField(self, label, field=None):
        return LabeledField(label, field, self)

    def _labeledLineEdit(self, label):
        return LabeledField(label, QLineEdit(parent=self), self)

    def _labeledCheckBox(self, label):
        return LabeledCheckBox(label, self)

    def _sampleDropDown(self, label, items=[]):
        return SampleDropDown(label, items, self)

    def _multiSelectDropDown(self, label, items=[]):
        return MultiSelectDropDown(label, items, self)

    def verify(self):
        raise NotImplementedError("The verification for this step was not completed.")

import json

from qtpy.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QMessageBox, QPushButton, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.SampleDropDown import SampleDropDown


class BackendRequestView(QWidget):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, jsonForm, selection, parent=None):
        super(BackendRequestView, self).__init__(parent)
        self.selection = selection
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        self.jsonForm = jsonForm
        self.layout.addWidget(jsonForm.widget, 2, 0, 1, 2)
        jsonForm.widget.setVisible(False)

    def getFieldText(self, key):
        return self.jsonForm.getField(key).text()

    def getField(self, key):
        return self.jsonForm.getField(key)

    def _labeledField(self, label, field):
        return LabeledField(label, field, self)

    def _sampleDropDown(self, label, items=[]):
        return SampleDropDown(label, items, self)

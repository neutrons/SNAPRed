import json

from qtpy.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QMessageBox, QPushButton, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.SampleDropDown import SampleDropDown


@Resettable
class IterateView(QWidget):
    def __init__(self, parent=None):
        super(IterateView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        self.message = QLabel("Repeat process and rename current results?")
        self.layout.addWidget(self.message, 0, 0, 1, 2)

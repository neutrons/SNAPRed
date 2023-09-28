from time import sleep

from qtpy.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.threading.worker_pool import WorkerPool


class LogTablePresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view, model):
        self.view = view
        self.model = model

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def _labelView(self, text):
        win = QWidget()
        label = QLabel(text)
        vbox = QVBoxLayout()
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        return win

    def _responseOK(self, response: SNAPResponse):
        return response.code - 200 < 100

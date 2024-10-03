from qtpy.QtWidgets import QLabel, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.ui.threading.worker_pool import WorkerPool


class LogTablePresenter(object):
    def __init__(self, view, model):
        # `InterfaceController` and `WorkerPool` are singletons:
        #   declaring them as instance attributes, rather than class attributes,
        #   allows singleton reset during testing.
        self.interfaceController = InterfaceController()
        self.worker_pool = WorkerPool()

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
        return response.code < ResponseCode.MAX_OK

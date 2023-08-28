import json

from PyQt5.QtWidgets import QGridLayout, QHBoxLayout, QLabel, QMessageBox, QPushButton, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.threading.worker_pool import WorkerPool


class BackendRequestView(QWidget):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, jsonForm, selection, parent=None):
        super(BackendRequestView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)
        self.jsonForm = jsonForm
        self.layout.addWidget(jsonForm.widget, 2, 0, 1, 2)

        self.beginFlowButton = QPushButton("Begin Operation")
        self.layout.addWidget(self.beginFlowButton, 3, 0, 1, 2)

        def commenceFlow():
            if selection.startswith("fitMultiplePeaks") or selection.startswith("vanadium"):
                return
            request = SNAPRequest(path=selection, payload=json.dumps(jsonForm.collectData()))
            self.handleButtonClicked(request, self.beginFlowButton)

        self.beginFlowButton.clicked.connect(commenceFlow)

    def getFieldText(self, key):
        return self.jsonForm.getField(key).text()

    def getField(self, key):
        return self.jsonForm.getField(key)

    def _labeledField(self, label, field):
        widget = QWidget()
        widget.setStyleSheet("background-color: #F5E9E2;")
        layout = QHBoxLayout()
        widget.setLayout(layout)

        label = QLabel(label)
        layout.addWidget(label)
        layout.addWidget(field)
        return widget

    def handleButtonClicked(self, reductionRequest, button):
        button.setEnabled(False)

        def executeRequest(reductionRequest):
            response = self.interfaceController.executeRequest(reductionRequest)
            if response.code != 200:
                ex = QWidget()
                QMessageBox.critical(ex, "Error! :^(", str(response.message))

        # setup workers with work targets and args
        self.worker = self.worker_pool.createWorker(target=executeRequest, args=(reductionRequest))

        # Final resets
        self.worker.finished.connect(lambda: button.setEnabled(True))

        self.worker_pool.submitWorker(self.worker)

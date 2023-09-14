import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.PromptUserforCalibrationInputView import PromptUserforCalibrationInputView


class CalibrationCheck(QObject):
    worker_pool = WorkerPool()
    interfaceController = InterfaceController()
    stateInitialized = pyqtSignal(SNAPResponse)

    def __init__(self, view):
        super().__init__()
        self.view = view
        self.message_widgets = []

    def _removePreviousMessages(self):
        for widget in self.message_widgets:
            self.view.layout().removeWidget(widget)
            widget.deleteLater()
        self.message_widgets.clear()

    def _labelView(self, text: str):
        self._removePreviousMessages()

        win = QWidget()
        vbox = QVBoxLayout()
        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        self.view.layout().addWidget(win)
        self.message_widgets.append(win)

    def handleButtonClicked(self):
        runNumber = self.view.getRunNumber()

        if not runNumber.isdigit():
            QMessageBox.warning(self.view, "Invalid Input", "Please enter a valid run number.")
            return

        self.view.beginFlowButton.setEnabled(False)
        runNumber_str = str(runNumber)

        stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload=runNumber_str)

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(stateCheckRequest,)
        )
        self.worker.result.connect(self.handleStateCheckResult)

        self.worker_pool.submitWorker(self.worker)

    def handleStateCheckResult(self, response: SNAPResponse):
        self.view.beginFlowButton.setEnabled(True)
        try:
            self.stateInitialized.disconnect()
        except TypeError:
            self._labelView(str(response.message))

        if response.data is False:
            self._spawnStateCreationWorkflow()
            self.stateInitialized.connect(self.handleStateCheckResult)
        else:
            self._labelView("Ready to Calibrate!")

    def _spawnStateCreationWorkflow(self):
        from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

        promptView = PromptUserforCalibrationInputView()
        promptView.setWindowModality(Qt.WindowModal)

        def pushDataToInterfaceController(run_number, state_name):
            payload = InitializeStateRequest(runId=run_number, humanReadableName=state_name)

            request = SNAPRequest(path="/calibration/initializeState", payload=payload.json())

            self.worker = self.worker_pool.createWorker(target=self.interfaceController.executeRequest, args=(request,))

            def handle_response(response: SNAPResponse):
                self.stateInitialized.emit(response)
                if response.code == 500:
                    self._labelView(str(response.message))
                else:
                    promptView.close()

            self.worker.result.connect(handle_response)

            self.worker_pool.submitWorker(self.worker)

        promptView.dataEntered.connect(pushDataToInterfaceController)
        promptView.exec_()

        # self.workflow = (
        #     WorkflowBuilder(self.view)
        #     .addNode(lambda workflow: None, promptView, "Calibration Input")  # noqa: ARG005
        #     .addNode(pushDataToInterfaceController, promptView, "Initialize State")
        #     .build()
        # )
        # self.workflow.show()

    @property
    def widget(self):
        return self.view

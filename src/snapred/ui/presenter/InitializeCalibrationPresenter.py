import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

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
        self.view.beginFlowButton.setEnabled(False)

        runNumber = self.view.getRunNumber()
        runNumber_str = str(runNumber)

        stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload=runNumber_str)

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(stateCheckRequest)
        )
        self.worker.result.connect(self.handleStateCheckResult)

        self.worker_pool.submitWorker(self.worker)

    def handleStateCheckResult(self, response: SNAPResponse):
        try:
            self.stateInitialized.disconnect()
        except TypeError:
            pass

        if response.data is False:
            self._spawnStateCreationWorkflow()
            self.stateInitialized.connect(self.handlePixelGroupingResult)
        else:
            runID = str(self.view.getRunNumber())
            pixelGroupingParametersRequest = SNAPRequest(path="/calibration/retrievePixelGroupingParams", payload=runID)

            self.worker = self.worker_pool.createWorker(
                target=self.interfaceController.executeRequest, args=(pixelGroupingParametersRequest)
            )
            self.worker.result.connect(self.handlePixelGroupingResult)

            self.worker_pool.submitWorker(self.worker)

    def _spawnStateCreationWorkflow(self):
        from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

        promptView = PromptUserforCalibrationInputView()
        promptView.setWindowModality(Qt.WindowModal)

        def pushDataToInterfaceController(run_number, state_name):
            payload = InitializeStateRequest(runId=run_number, humanReadableName=state_name)

            request = SNAPRequest(path="/calibration/initializeState", payload=payload.json())

            self.worker = self.worker_pool.createWorker(target=self.interfaceController.executeRequest, args=(request))

            def handle_response(response: SNAPResponse):
                self.stateInitialized.emit(response)

            self.worker.result.connect(handle_response)

            self.worker_pool.submitWorker(self.worker)

        promptView.dataEntered.connect(pushDataToInterfaceController)
        promptView.show()
        self.workflow = (
            WorkflowBuilder(self.view)
            .addNode(lambda workflow: None, promptView, "Calibration Input")  # noqa: ARG005
            .addNode(pushDataToInterfaceController, promptView, "Initialize State")
            .build()
        )
        self.workflow.show()
        promptView.close()

    def handlePixelGroupingResult(self, response: SNAPResponse):
        self.view.beginFlowButton.setEnabled(True)
        if response.code == 200:
            self._labelView("Ready to Calibrate!")
            self.view.beginFlowButton.setEnabled(True)
        else:
            self._labelView(str(response.message))

    @property
    def widget(self):
        return self.view

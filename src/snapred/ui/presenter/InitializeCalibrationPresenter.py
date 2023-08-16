import json

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.dao.StateConfig import StateConfig
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.PromptUserforCalibrationInputView import PromptUserforCalibrationInputView


class CalibrationCheck(object):
    worker_pool = WorkerPool()
    interfaceController = InterfaceController()

    def __init__(self, view):
        self.view = view

    def _labelView(self, text: str):
        win = QWidget()
        vbox = QVBoxLayout()
        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        self.view.layout().addWidget(win)

    def handleButtonClicked(self):
        self.view.beginFlowButton.setEnabled(False)

        runNumber = self.view.getRunNumber()
        runNumber_str = str(runNumber)

        stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload=json.dumps({"runNumber": runNumber_str}))

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(stateCheckRequest)
        )
        self.worker.result.connect(self.handleStateCheckResult)

        self.worker_pool.submitWorker(self.worker)

    def handleStateCheckResult(self, response: SNAPResponse):
        if response.responseCode != 200:
            self._spawnStateCreationWorkflow()
            return
        else:
            pass

        runID = str(self.view.getRunNumber())
        pixelGroupingParametersRequest = SNAPRequest(
            path="/calibration/retrievePixelGroupingParams", payload=json.dumps({"runNumber": runID})
        )

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(pixelGroupingParametersRequest)
        )
        self.worker.result.connect(self.handlePixelGroupingResult)

        self.worker_pool.submitWorker(self.worker)

    def handlePixelGroupingResult(self, response: SNAPResponse):
        if response.responseCode == 200:
            self._labelView("Ready to Calibrate!")

    def _spawnStateCreationWorkflow(self):
        from snapred.ui.widget.WorkflowNode import continueWorkflow, finalizeWorkflow, startWorkflow

        promptView = PromptUserforCalibrationInputView()

        def pushDataToInterfaceController():
            run_number = promptView.getRunNumber()
            state_name = promptView.getName()

            payload = {"runNumber": run_number, "stateName": state_name}

            request = SNAPRequest(path="/calibration/initializeState", payload=payload)

            self.worker = self.worker_pool.createWorker(target=self.interfaceController.executeRequest, args=(request,))

            def handle_response():
                pass

            self.worker.result.connect(handle_response)

            self.worker_pool.submitWorker(self.worker)

        Start = startWorkflow(None, promptView)
        Continue = continueWorkflow(Start, pushDataToInterfaceController, promptView)
        Finish = finalizeWorkflow(Continue)

        Finish.presenter.show()

    @property
    def widget(self):
        return self.view

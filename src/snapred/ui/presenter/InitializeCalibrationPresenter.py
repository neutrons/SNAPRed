from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QVBoxLayout, QLabel, QWidget, QLineEdit

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.view.PromptUserforCalibrationInputView import PromptUserforCalibrationInputView

class CalibrationCheck(object):
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
        self.view.layout.addWidget(win)

    def handleButtonClicked(self, response: SNAPResponse):
        self.view.beginFlowButton.setEnabled(False)

        # Check to see if data exists
        runNumber = self.view.getRunNumber()
        runNumber_str = str(runNumber)
        dataCheckRequest = SNAPRequest(path="/calibration/checkDataExists", payload=runNumber_str)
        response = InterfaceController.executeRequest(dataCheckRequest)

        if response.responseCode != 200:
            self._labelView("Error, data doesn't exist")
            return

        stateCheckRequest = SNAPRequest(path="/calibration/hasState", payload=runNumber_str)
        response = InterfaceController.executeRequest(stateCheckRequest)

        if response.responseCode != 200:
            self._spawnStateCreationWorkflow()

        groupingFile = str(StateConfig.focusGroups.definition)

        pixelGroupingParametersRequest = SNAPRequest("/calibration/calculatePixelGroupingParameters", groupingFile)

        if pixelGroupingParametersRequest:
            self._labelView("Ready to Calibrate!")

    def _spawnStateCreationWorkflow(self):
        from snapred.ui.widget.WorkflowNode import startWorkflow, continueWorkflow, finalizeWorkflow

        promptView = PromptUserforCalibrationInputView()
        Start = startWorkflow(None, promptView)
        Continue = continueWorkflow(Start, lambda workflow: """push data to interfacecontroller""", promptView)
        Finish = finalizeWorkflow(Continue)

        Finish.presenter.show()

    @property
    def widget(self):
        return self.view
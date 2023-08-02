from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton, QLineEdit

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.view.PromptUserforCalibrationInputView import PromptUserforCalibrationInputView
from snapred.ui.widget.JsonForm import JsonForm

class CalibrationCheck(object):
    stateConfig = StateConfig()
    jsonForm = JsonForm()

    def __init__(self, view):
        self.view = view

        self.view.on_button_clicked(self.handleButtonClicked, view)

    def _labelView(self, text: str):
        win = QWidget()
        win.setStyleSheet("background-color: #F5E9E2;")
        vbox = QVBoxLayout()
        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        return win

    def handleButtonClicked(self, responce: SNAPResponse):
        self.view.button.setEnabled(False)

        # check to see if data exists
        runNumber = self.view.getRunNumber()
        dataCheckRequest = SNAPRequest(path = "/calibration/checkDataExists", payload = {"runNumber": runNumber})
        responce = InterfaceController.executeRequest(dataCheckRequest)

        if responce.responseCode != 200:
            self._labelView("Error, data doesn't exist")

        stateCheckRequest = SNAPRequest(path = "/calibration/hasState", payload = {"runNumber": runNumber})
        responce = InterfaceController.executeRequest(stateCheckRequest)

        if responce.responseCode != 200:
            self._spawnStateCreationWorkflow()
            self.handleButtonClicked()

        groupingFile = str(self.stateConfig.focusGroups.definition)

        pixelGroupingParametersRequest = SNAPRequest("/calibration/calculatePixelGroupingParameters", groupingFile)

        if pixelGroupingParametersRequest:
            self._labelView("Ready to Calibrate!")

    def _spawnStateCreationWorkflow(self, response: SNAPResponse):
        if response.responseCode == 200:
            from snapred.ui.widget.WorkflowNode import startWorkflow, continueWorkflow, finalizeWorkflow
        
            promptView = PromptUserforCalibrationInputView()
            Start = startWorkflow(None, promptView)
            Continue = continueWorkflow(Start, lambda workflow: """push data to interfacecontroller""", promptView)
            Finish = finalizeWorkflow(Continue)

            Finish.presenter.show()

    @property
    def widget(self):
        return self.view
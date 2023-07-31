import os
import json
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.widget.WorkflowNode import finalizeWorkflow, startWorkflow



class InitializeCalibrationCheckView(BackendRequestView):

    def __init__(self, jsonForm, parent=None):
        selection = "calibration/initializeCalibrationCheck"
        super(InitializeCalibrationCheckView, self).__init__(jsonForm, selection, parent=parent)
        runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self))
        self.layout.addWidget(runNumberField, 0, 0)
        self.layout.addWidget(litemodeToggle, 0, 1)

        sampleDropdown = QComboBox()
        sampleDropdown.addItem("Select Sample")
        # todo: get samples from backend
        groupingFileDropdown = QComboBox()
        groupingFileDropdown.addItem("Select Grouping File")
        # todo get grouping files from backend
        self.layout.addWidget(sampleDropdown, 1, 0)
        self.layout.addWidget(groupingFileDropdown, 1, 1)
        self.beginFlowButton = QPushButton("Check")
        self.layout.addWidget(self.beginFlowButton, 3, 0, 1, 2)
        
        def initializeCalibrationCheckFlow():
            request = SNAPRequest(path = selection, payload = json.dumps(jsonForm.collectData()))
            self.handleButtonClicked(request, self.beginFlowButton)

            def showResult(resultText: str, needText: bool):
                workflow = startWorkflow(lambda workflow: None, self._labelView(resultText, needText))
                workflow = finalizeWorkflow(workflow, self)
                workflow.widget.show()
            
            self.worker.finished.connect(lambda: showResult("Test", True))

        self.beginFlowButton.clicked.connect(initializeCalibrationCheckFlow)

    def _labelView(self, text: str, needText: bool):
        win = QWidget()
        win.setStyleSheet("background-color: #F5E9E2;")

        vbox = QVBoxLayout()

        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        vbox.addWidget(label)

        if needText:
            name_input = QLineEdit()
            name_input.setPlaceholderText("Enter name for state")
            vbox.addWidget(name_input)

        vbox.addStretch()
        win.setLayout(vbox)
        return win
    
    def handleButtonClicked(self, intializationRequest, button):
        button.setEnabled(False)

        def executeRequest(initializationRequest):
            response = self.interfaceController.executeRequest(initializationRequest)
            if response.responseCode != 200:
                ex = QWidget()
                QMessageBox.critical(ex, "Error", str(response.responseMessage))

            elif response.responseData == "success":
                self.showResult("Ready to Calibrate!", False)
                
            elif response.responseData == "needText":
                self.showResult("Please enter name for State!", True)
                # return self.name_input.text()
                
        self.worker = self.worker_pool.createWorker(target=executeRequest, args=(intializationRequest))

        self.worker.finished.connect(lambda: button.setEnabled(True))

        self.worker_pool.submitWorker(self.worker)

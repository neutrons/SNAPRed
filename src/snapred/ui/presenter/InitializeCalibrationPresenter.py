import json

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton, QLineEdit

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.widget.JsonForm import JsonForm

class CalibrationCheck(object):
    runConfig = RunConfig()
    stateConfig = StateConfig()
    worker_pool = WorkerPool()
    interfaceController = InterfaceController()
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


    def _spawnStateCreationWorkflow(self):
        
        

    @property
    def widget(self):
        return self.view
        
    #     request = SNAPRequest(path = selection, payload = json.dumps(jsonForm.collectData()))
    #     self.handleButtonClicked(request, self.beginFlowButton)

    #     def showResult(resultText: str, needText: bool):
    #         workflow = startWorkflow(lambda workflow: None, self._labelView(resultText, needText))
    #         workflow = finalizeWorkflow(workflow, self)
    #         workflow.widget.show()
        
    #     self.worker.finished.connect(lambda: showResult("Test", True))

    #     self.beginFlowButton.clicked.connect(self.initializeCalibrationCheckFlow)

    # def _labelView(self, text: str, needText: bool):
    #     win = QWidget()
    #     win.setStyleSheet("background-color: #F5E9E2;")

    #     vbox = QVBoxLayout()

    #     label = QLabel(text)
    #     label.setAlignment(Qt.AlignCenter)
    #     vbox.addWidget(label)

    #     if needText:
    #         name_input = QLineEdit()
    #         name_input.setPlaceholderText("Enter name for state")
    #         vbox.addWidget(name_input)
    #         print(name_input.text)
        
    #     # else:
    #     #     name = None
    #     vbox.addStretch()
    #     win.setLayout(vbox)
    #     return win #, name
    
    # def handleButtonClicked(self, initializeCalibrationCheck, button):
    #     button.setEnabled(False)

    #     def executeRequest(initializeCalibrationCheck):
    #         response = self.interfaceController.executeRequest(initializeCalibrationCheck)
    #         if response.responseCode != 200:
    #             ex = QWidget()
    #             QMessageBox.critical(ex, "Error", str(response.responseMessage))

    #         elif response.responseData == "success":
    #             self.showResult("Ready to Calibrate!", False)
                
    #         elif response.responseData == "needText":
    #             self.showResult("Please enter name for State!", True)
    #             self.interfaceController.executeRequest(initializeCalibrationCheck)
                
    #     self.worker = self.worker_pool.createWorker(target=executeRequest, args=(initializeCalibrationCheck))

    #     self.worker.finished.connect(lambda: button.setEnabled(True))

    #     self.worker_pool.submitWorker(self.worker)

    # def initializeCalibrationCheck(self):
    #     if not runs:
    #         raise ValueError("List is empty")
    #     else:
    #         # list to store states
    #         states = []
    #         for run in runs:
    #             # identify the instrument state for measurement
    #             state = self.dataFactoryService.getStateConfig(run.runNumber)
    #             states.append(state)
    #             # check if state exists and create in case it does not exist
    #             for state in states:
    #                 hasState = self.hasState(state, "*")
    #                 if not hasState:
    #                     self.promptUserForName()
    #                 # elif name is not None:
    #                 #     request = InitializeStateRequest(run.runNumber, name)
    #                 #     self.initializeState(request)

    #             reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
    #             groupingFile = reductionIngredients.reductionState.stateConfig.focusGroups.definition
    #             # calculate pixel grouping parameters
    #             pixelGroupingParameters = self.calculatePixelGroupingParameters(
    #                     runs, groupingFile
    #                 )
    #             if pixelGroupingParameters:
    #                 success = str("success")
    #                 return success
    #             else:
    #                 raise Exception("Error in calculating pixel grouping parameters")
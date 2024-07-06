from qtpy.QtCore import QObject, Signal
from qtpy.QtWidgets import QMessageBox

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.ui.widget.LoadingCursor import LoadingCursor
from snapred.ui.widget.SuccessPrompt import SuccessPrompt


class InitializeStatePresenter(QObject):
    """
    Manages interactions between the UI and the backend for state initialization processes.

    This presenter handles user inputs from the UI, validates them, and initiates requests to the backend
    to initialize the state of an instrument or a process based on provided parameters. It updates the UI
    based on the outcomes of these requests.
    """

    stateInitialized = Signal(SNAPResponse)

    def __init__(self, view):
        super().__init__()
        self.view = view
        self.interfaceController = InterfaceController()

    def handleButtonClicked(self):
        runNumber = self.view.getRunNumber()
        stateName = self.view.getStateName()
        useLiteMode = self.view.getMode()

        if not runNumber.isdigit():
            QMessageBox.warning(self.view, "Invalid Input", "Please enter a valid run number.")
            return

        self.view.beginFlowButton.setEnabled(False)
        self._initializeState(runNumber, stateName, useLiteMode)

    def _initializeState(self, runNumber, stateName, useLiteMode):
        self.loadingCursor = LoadingCursor(self.view)
        self.loadingCursor.show()
        payload = InitializeStateRequest(runId=str(runNumber), humanReadableName=stateName, useLiteMode=useLiteMode)
        request = SNAPRequest(path="/calibration/initializeState", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self._handleResponse(response)

    def _handleResponse(self, response: SNAPResponse):
        self.view.beginFlowButton.setEnabled(True)
        if response.code == ResponseCode.ERROR:
            QMessageBox.critical(self.view, "Error", "Error: " + response.message)
            self.loadingCursor.close()
        else:
            self.stateInitialized.emit(response)
            self.loadingCursor.close()
            SuccessPrompt.prompt(self.view)

"""
User Input:

    Run number of normization data set
    Corresponding calibrant sample (vanadium or V-Nb)
    Run number of empty instrument data set
    Input parameters for absorption correction algo
    Input parameters for new strip peaks algo

Output:

    Raw vanadium data file -> stored in calibration folder
"""
import json

from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest, SNAPResponse
from snapred.backend.log.logger import snapredLogger
from snapred.backend.dao.request import(
    NormalizationCalibrationRequest,
    SpecifyCalibrationRequest,
)
from snapred.ui.view.NormalizationCalibrationRequestView import NormalizationCalibrationRequestView
from snapred.ui.view.SpecifyNormalizationCalibrationView import SpecifyNormalizationCalibrationView
from snapred.ui.view.SaveNormalizationCalibrationView import SaveNormalizationCalibrationView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

class NormalizationCalibrationWorkflow:
    def __init__(self, jsonForm, parent=None):
        self.requests = []
        self.responses = []
        self.interfaceController = InterfaceController()
        request = SNAPRequest(path="api/parameters", payload="calibration/normalization")
        self.assessmentSchema = self.interfaceController.executeRequest(request).data
        self.assessmentSchema = {key: json.loads(value) for key, value in self.assessmentSchema}

        request = SNAPRequest(path="api/parameters", payload="calibration/saveNormalization")
        self.saveSchema = self.interfaceController.executeRequest(request).data
        self.saveSchema = {key: json.loads(value) for key, value in self.saveSchema.items()}
        cancelLambda = None
        if parent is not None and hasattr(parent, "close"):
            cancelLambda = parent.close
        
        request = SNAPRequest(path="config/calibrantSamples")
        self.calibrantSamples = self.interfaceController.executeRequest(request).data

        self._normalizationCalibrationView = NormalizationCalibrationRequestView(
            jsonForm, self.calibrantSamples, parent=parent
        )

        self._specifyCalibrationView = SpecifyNormalizationCalibrationView(
            "Specifying Calibration", self.assessmentSchema, parent=parent
        )

        self._saveNormalizationCalibrationView = SaveNormalizationCalibrationView("Saving Normalization Calibration", self.saveSchema, parent)

        self.workflow = (
            WorkflowBuilder(cancelLambda=cancelLambda, parent=parent)
            .addNode(
                self._triggerNormalizationCalibration,
                self._normalizationCalibrationView,
                "Normalization Calibration",
            )
            .addNode(
                self._specifyCalibration,
                self._specifyCalibrationView,
                "Specify Calibration"
            )
            .addNode(
                self._saveNormalizationCalibration,
                self._saveNormalizationCalibrationView,
                "Saving"
            )
            .build()
        )

    def _triggerNormalizationCalibration(self, workflowPresenter):
        view = workflowPresenter.widget.tabview

        try:
            view.verify()
        except ValueError as e:
            return SNAPResponse(code=500, message=f"Missing Fields!{e}")
        
        self.runNumber = view.getFieldText("runNumber")
        self.emptyRunNumber = view.getFieldText("emptyRunNumber")
        calibrantIndex = view.calibrantIndex.currentIndex()

        self._specifyCalibrationView.updateCalibrantSample(calibrantIndex)
        self._specifyCalibrationView.updateRunNumber(self.runNumber)
        self._specifyCalibrationView.updateEmptyRunNumber(self.emptyRunNumber)
        self._saveNormalizationCalibrationView.updateRunNumber(self.runNumber)
        self._calibrantPath = view.calibrantPath.currentText()

        payload = NormalizationCalibrationRequest(
            runNumber=self.runNumber, emptyRunNumber=self.emptyRunNumber, calibrantPath=self._calibrantPath
        )
        payload.convergenceThreshold = view.fieldConvergenceThreshold.get(payload.convergenceThreshold)
        
        request = SNAPRequest(path="calibration/normalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response
    
    def _specifyCalibration(self, workflowPresenter):
        payload = SpecifyCalibrationRequest(
            run=RunConfig(runNumber=self.runNumber),
            workspace=self.responses[-1].data["outputWorkspace"]
            smoothWorkspace=self.responses[-2].data["smoothOutputWorkspace"]
        )
        request = SNAPRequest(path="calibration/specifyNormalization", payload=payload.json())
        response = self.interfaceController.executeRequest(request)
        self.responses.append(response)
        return response
    
    def _saveNormalizationCalibration(self, workflowPresenter):

        #symlink?
    
    @property
    def widget(self):
        return self.workflow.presenter.widget
    
    def show(self):
        pass
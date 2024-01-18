from PyQt5.QtCore import QObject, Qt, pyqtSignal
from PyQt5.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request.CalibrationLoadAssessmentRequest import CalibrationLoadAssessmentRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.ui.threading.worker_pool import WorkerPool


class CalibrationAssessmentLoader(QObject):
    worker_pool = WorkerPool()
    interfaceController = InterfaceController()

    assessmentLoaded = pyqtSignal(SNAPResponse)
    """Emit when calibration assessment has been loaded.

    :param SNAPResponse: The response from loading calibration assessment
    """

    def __init__(self, view):
        super().__init__()
        self.view = view

    def handleLoadRequested(self):
        if self.view.getCalibrationRecordCount() < 1:
            self.view.onLoadError("No calibration records available.")
            return

        if self.view.getSelectedCalibrationRecordIndex() < 0:
            self.view.onLoadError("No calibration record selected.")
            return

        runId, version = self.view.getSelectedCalibrationRecordData()
        payload = CalibrationLoadAssessmentRequest(runId=runId, version=version, checkExistent=True)
        loadAssessmentRequest = SNAPRequest(path="/calibration/loadQualityAssessment", payload=payload.json())

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(loadAssessmentRequest)
        )
        self.worker.result.connect(self.handleLoadAssessmentResult)
        self.worker_pool.submitWorker(self.worker)

    def handleLoadAssessmentResult(self, response: SNAPResponse):
        if response.code == 500:
            self.view.onLoadError(response.message)

    @property
    def widget(self):
        return self.view

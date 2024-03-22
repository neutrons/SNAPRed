from qtpy.QtCore import QObject, Qt, Signal
from qtpy.QtWidgets import QLabel, QMessageBox, QVBoxLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao import RunConfig, SNAPRequest
from snapred.backend.dao.request import (
    CalibrationIndexRequest,
    CalibrationLoadAssessmentRequest,
)
from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.ui.threading.worker_pool import WorkerPool


class CalibrationAssessmentPresenter(QObject):
    """

    The CalibrationAssessmentPresenter is a component designed to bridge user interactions with
    the underlying calibration assessment and indexing processes. Leveraging a WorkerPool for
    asynchronous task execution and an InterfaceController for API communication, it manages
    user requests from the UI to load specific calibration assessments and the calibration index
    for a given run number. Upon user action, it initiates requests, such as loading selected
    calibration assessments based on run ID and version, and updating the UI with the results
    or error messages as appropriate. This setup allows for non-blocking UI operations,
    enhancing the application's responsiveness.

    """

    worker_pool = WorkerPool()
    interfaceController = InterfaceController()

    def __init__(self, view):
        super().__init__()
        self.view = view

    def loadSelectedCalibrationAssessment(self):
        if self.view.getCalibrationRecordCount() < 1:
            self.view.onError("No calibration records available.")
            return

        if self.view.getSelectedCalibrationRecordIndex() < 0:
            self.view.onError("No calibration record selected.")
            return

        runId, version = self.view.getSelectedCalibrationRecordData()
        payload = CalibrationLoadAssessmentRequest(runId=runId, version=version, checkExistent=True)
        loadAssessmentRequest = SNAPRequest(path="/calibration/loadQualityAssessment", payload=payload.json())

        self.view.loadButton.setEnabled(False)
        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(loadAssessmentRequest)
        )
        self.worker.finished.connect(lambda: self.view.loadButton.setEnabled(True))
        self.worker.result.connect(self.handleLoadAssessmentResult)
        self.worker_pool.submitWorker(self.worker)

    def handleLoadAssessmentResult(self, response: SNAPResponse):
        if response.code == ResponseCode.ERROR:
            self.view.onError(response.message)

    def loadCalibrationIndex(self, runNumber: str):
        payload = CalibrationIndexRequest(
            run=RunConfig(runNumber=runNumber),
        )
        loadCalibrationIndexRequest = SNAPRequest(path="calibration/index", payload=payload.json())

        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(loadCalibrationIndexRequest)
        )
        self.worker.result.connect(self.handleLoadCalibrationIndexResult)
        self.worker_pool.submitWorker(self.worker)

    def handleLoadCalibrationIndexResult(self, response: SNAPResponse):
        if response.code == ResponseCode.ERROR:
            self.view.onError(response.message)
        else:
            self.view.updateCalibrationRecordList(response.data)

    @property
    def widget(self):
        return self.view

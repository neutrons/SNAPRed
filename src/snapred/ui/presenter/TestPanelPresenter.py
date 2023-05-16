from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.ui.threading.worker_pool import WorkerPool


class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        self.view = view

        self.view.calibrationReductinButtonOnClick(self.handleCalibrationReductinButtonClicked)
        self.view.calibrationIndexButtonOnClick(self.handleCalibrationIndexButtonClicked)

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def handleCalibrationReductinButtonClicked(self):
        reductionRequest = SNAPRequest(path="calibration/reduction", payload=RunConfig(runNumber="57514").json())
        self.handleButtonClicked(reductionRequest, self.view.calibrationReductinButton)

    def handleCalibrationIndexButtonClicked(self):
        reductionRequest = SNAPRequest(
            path="calibration/save",
            payload=CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author").json(),
        )
        self.handleButtonClicked(reductionRequest, self.view.calibrationIndexButton)

    def handleButtonClicked(self, reductionRequest, button):
        button.setEnabled(False)

        # setup workers with work targets and args
        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(reductionRequest)
        )

        # Final resets
        self.worker.finished.connect(lambda: button.setEnabled(True))

        self.worker_pool.submitWorker(self.worker)

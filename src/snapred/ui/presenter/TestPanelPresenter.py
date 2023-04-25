from PyQt5.QtWidgets import QMessageBox
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.ui.threading.worker_pool import WorkerPool

class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        self.view = view

        self.view.calibrationReductinButtonOnClick(self.handleCalibrationReductinButtonClicked)

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def handleCalibrationReductinButtonClicked(self):
        reductionRequest = SNAPRequest(mode="Calibration Reduction", runs=[RunConfig(runNumber="57514")])
        self.handleButtonClicked(reductionRequest, self.view.calibrationReductinButton)

    def handleButtonClicked(self, reductionRequest, button):
        button.setEnabled(False)

        # setup workers with work targets and args
        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(reductionRequest)
        )

        # Final resets
        self.worker.finished.connect(lambda: button.setEnabled(True))

        self.worker_pool.submitWorker(self.worker)

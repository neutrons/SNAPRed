from time import sleep

from PyQt5.QtWidgets import QMessageBox

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.ReductionRequest import ReductionRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.ui.threading.worker_pool import WorkerPool


class Spinner:
    i = 0
    symbols = ["|", "/", "-", "\\"]

    def getAndIterate(self):
        symbol = self.symbols[self.i % 4]
        self.i += 1
        if self.i > 3:
            self.i = 0
        return symbol


class LogTablePresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view, model):
        self.view = view
        self.model = model

        self.view.on_button_clicked(self.handle_button_clicked)

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def update_reduction_config_element(self, reductionResponse):
        # import pdb; pdb.set_trace()
        if reductionResponse.responseCode == 200:
            pass
        else:
            messageBox = QMessageBox()
            messageBox.setIcon(QMessageBox.Critical)
            messageBox.setText(reductionResponse.responseMessage)
            messageBox.setFixedSize(500, 200)
            messageBox.exec()

    def buttonSpinner(self, spinner):
        sleep(0.25)
        return "Loading " + spinner.getAndIterate()

    def updateButton(self, text):
        self.view.button.setText(text)

    def handle_button_clicked(self):
        self.view.button.setEnabled(False)

        reductionRequest = ReductionRequest(mode="Reduction", runs=[RunConfig(runNumber="48741")])

        # setup workers with work targets and args
        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(reductionRequest)
        )
        self.infworker = self.worker_pool.createInfiniteWorker(target=self.buttonSpinner, args=(Spinner()))

        # update according to results
        self.infworker.result.connect(self.updateButton)
        self.worker.result.connect(self.update_reduction_config_element)

        # Final resets
        self.worker.finished.connect(self.infworker.stop)
        self.worker.finished.connect(lambda: self.view.button.setEnabled(True))
        self.infworker.finished.connect(lambda: self.infworker.stop())
        self.infworker.finished.connect(lambda: self.updateButton("load dummy"))

        self.worker_pool.submitWorker(self.worker)
        self.worker_pool.submitWorker(self.infworker)

from time import sleep

from snapred.ui.threading.worker_pool import WorkerPool


class _Spinner:
    i = 0
    symbols = ["|", "/", "-", "\\"]

    def getAndIterate(self):
        symbol = self.symbols[self.i % 4]
        self.i += 1
        if self.i > 3:
            self.i = 0
        return symbol


class WorkflowPresenter(object):
    worker_pool = WorkerPool()

    def __init__(self, view, model):
        self.view = view
        self.model = model

        self.view.onContinueButtonClicked(self.handleContinueButtonClicked)
        self.view.onQuitButtonClicked(self.handleQuitButtonClicked)

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def updateSubview(self):
        self.view.updateSubview(self.model.view)

    def handleContinueButtonClicked(self):
        self.view.continueButton.setEnabled(False)
        self.view.quitButton.setEnabled(False)

        if self.model.nextModel is None:
            return self.handleQuitButtonClicked()

        self.model = self.model.nextModel

        # do action
        self.worker = self.worker_pool.createWorker(target=self.model.action, args=(self))
        self.worker.finished.connect(self.updateSubview)
        self.worker.finished.connect(lambda: self.view.continueButton.setEnabled(True))
        self.worker.finished.connect(lambda: self.view.quitButton.setEnabled(True))

        self.infworker = self.worker_pool.createInfiniteWorker(target=self._buttonSpinner, args=(_Spinner()))

        # update according to results
        self.infworker.result.connect(self._updateButton)

        # Final resets
        self.worker.finished.connect(self.infworker.stop)
        self.infworker.finished.connect(lambda: self.infworker.stop())
        self.infworker.finished.connect(lambda: self._updateButton("Continue \U00002705"))

        self.worker_pool.submitWorker(self.infworker)
        self.worker_pool.submitWorker(self.worker)

    def handleQuitButtonClicked(self):
        self.view.close()

    def _updateButton(self, text):
        self.view.button.setText(text)

    def _buttonSpinner(self, spinner):
        sleep(0.25)
        return "Loading " + spinner.getAndIterate()

from snapred.ui.threading.worker_pool import WorkerPool


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

        self.worker_pool.submitWorker(self.worker)

    def handleQuitButtonClicked(self):
        self.view.close()

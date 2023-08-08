from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView


class WorkflowPresenter(object):
    worker_pool = WorkerPool()

    def __init__(self, model: WorkflowNodeModel, parent=None):
        self.view = WorkflowView(model, parent)
        self.model = model
        self._hookupSignals()

    @property
    def widget(self):
        return self.view

    def _hookupSignals(self):
        for i, model in enumerate(self.model):
            widget = self.view.tabWidget.widget(i)
            widget.onContinueButtonClicked(lambda: self.handleContinueButtonClicked(model))
            widget.onCancelButtonClicked(self.view.deleteLater)

    def handleContinueButtonClicked(self, model):
        self.view.continueButton.setEnabled(False)
        self.view.cancelButton.setEnabled(False)

        # do action
        self.worker = self.worker_pool.createWorker(target=model.continueAction, args=(self))
        self.worker.finished.connect(lambda: self.view.continueButton.setEnabled(True))
        self.worker.finished.connect(lambda: self.view.cancelButton.setEnabled(True))
        self.worker.success.connect(lambda success: self.view.advanceWorkflow() if success else None)

        self.worker_pool.submitWorker(self.worker)

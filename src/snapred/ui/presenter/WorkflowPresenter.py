from qtpy.QtWidgets import QMainWindow

from snapred.backend.log.logger import snapredLogger
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(object):
    worker_pool = WorkerPool()

    def __init__(self, model: WorkflowNodeModel, parent=None):
        self.view = WorkflowView(model, parent)
        self.model = model
        self._hookupSignals()

    @property
    def widget(self):
        return self.view

    def show(self):
        # wrap view in QApplication
        self.window = QMainWindow(self.view.parent())
        self.window.setCentralWidget(self.view)
        self.window.show()

    def _hookupSignals(self):
        for i, model in enumerate(self.model):
            widget = self.view.tabWidget.widget(i)
            logger.debug(
                f"Hooking up signals for tab {widget.view} to {widget.model.continueAction} \
                    to continue button {widget.continueButton}"
            )
            widget.onContinueButtonClicked(self.handleContinueButtonClicked)
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

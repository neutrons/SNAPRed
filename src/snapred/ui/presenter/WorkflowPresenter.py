from qtpy.QtWidgets import QMainWindow, QMessageBox

from snapred.backend.log.logger import snapredLogger
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.WorkflowView import WorkflowView

logger = snapredLogger.getLogger(__name__)


class WorkflowPresenter(object):
    worker_pool = WorkerPool()

    def __init__(self, model: WorkflowNodeModel, cancelLambda=None, parent=None):
        self.view = WorkflowView(model, parent)
        self.model = model
        self._cancelLambda = cancelLambda
        self._hookupSignals()

    @property
    def widget(self):
        return self.view

    @property
    def nextView(self):
        return self.view.nextTabView

    def show(self):
        # wrap view in QApplication
        self.window = QMainWindow(self.view.parent())
        self.window.setCentralWidget(self.view)
        self.window.show()

    def resetSoft(self):
        self.widget.reset(hard=False)

    def resetHard(self):
        self.widget.reset(hard=True)

    def _hookupSignals(self):
        for i, model in enumerate(self.model):
            widget = self.view.tabWidget.widget(i)
            logger.debug(
                f"Hooking up signals for tab {widget.view} to {widget.model.continueAction} \
                    to continue button {widget.continueButton}"
            )
            widget.onContinueButtonClicked(self.handleContinueButtonClicked)
            if self._cancelLambda:
                widget.onCancelButtonClicked(self._cancelLambda)
            else:
                widget.onCancelButtonClicked(self.resetHard)

    def handleContinueButtonClicked(self, model):
        self.view.continueButton.setEnabled(False)
        self.view.cancelButton.setEnabled(False)

        # do action
        self.worker = self.worker_pool.createWorker(target=model.continueAction, args=(self))
        self.worker.finished.connect(lambda: self.view.continueButton.setEnabled(True))
        self.worker.finished.connect(lambda: self.view.cancelButton.setEnabled(True))
        self.worker.result.connect(self._handleComplications)
        self.worker.success.connect(lambda success: self.view.advanceWorkflow() if success else None)

        self.worker_pool.submitWorker(self.worker)

    def _handleComplications(self, result):
        if result.code - 200 >= 100:
            QMessageBox.critical(
                self.view,
                "Error",
                f"Error {result.code}: {result.message}",
                QMessageBox.Ok,
                QMessageBox.Ok,
            )
        elif result.message:
            messageBox = QMessageBox(
                QMessageBox.Warning,
                "Warning",
                "Proccess completed successfully with warnings!",
                QMessageBox.Ok,
                self.view,
            )
            messageBox.setDetailedText(f"{result.message}")
            messageBox.exec()

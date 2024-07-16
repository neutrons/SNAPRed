from qtpy.QtWidgets import QGridLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.log.logger import snapredLogger
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.workflow.DiffCalWorkflow import DiffCalWorkflow
from snapred.ui.workflow.NormalizationWorkflow import NormalizationWorkflow
from snapred.ui.workflow.ReductionWorkflow import ReductionWorkflow

logger = snapredLogger.getLogger(__name__)


class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        self.view = view

        self.diffractionCalibrationWidget = self._createWorkflowWidget(self._createDiffCalWorkflow)
        self.calibrationNormalizationWidget = self._createWorkflowWidget(self._createNormalizationWorkflow)
        self.reductionWidget = self._createWorkflowWidget(self._createReductionWorkflow)

        self.view.tabWidget.addTab(self.diffractionCalibrationWidget, "Diffraction Calibration")
        self.view.tabWidget.addTab(self.calibrationNormalizationWidget, "Normalization")
        self.view.tabWidget.addTab(self.reductionWidget, "Reduction")

    def _createWorkflowWidget(self, method):
        layout = QGridLayout()
        widget = QWidget()
        widget.setLayout(layout)
        layout.addWidget(method())
        return widget

    def _createDiffCalWorkflow(self):
        return DiffCalWorkflow(parent=self.view).widget

    def _createNormalizationWorkflow(self):
        return NormalizationWorkflow(parent=self.view).widget

    def _createReductionWorkflow(self):
        return ReductionWorkflow(parent=self.view).widget

    @property
    def widget(self):
        return self.view

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

        # For testing purposes:
        #   here we retain references to the workflows themselves, and not just their widgets,
        #   so that we can access their signals.
        self.diffractionCalibrationWorkflow = DiffCalWorkflow(parent=self.view)
        self.normalizationCalibrationWorkflow = NormalizationWorkflow(parent=self.view)
        self.reductionWorkflow = ReductionWorkflow(parent=self.view)

        self.diffractionCalibrationWidget = self._addWorkflowWidget(self.diffractionCalibrationWorkflow.widget)
        self.normalizationCalibrationWidget = self._addWorkflowWidget(self.normalizationCalibrationWorkflow.widget)
        self.reductionWidget = self._addWorkflowWidget(self.reductionWorkflow.widget)

        self.view.tabWidget.addTab(self.diffractionCalibrationWidget, "Diffraction Calibration")
        self.view.tabWidget.addTab(self.normalizationCalibrationWidget, "Normalization")
        self.view.tabWidget.addTab(self.reductionWidget, "Reduction")

    def _addWorkflowWidget(self, widget_):
        layout = QGridLayout()
        widget = QWidget()
        widget.setLayout(layout)
        layout.addWidget(widget_)
        return widget

    @property
    def widget(self):
        return self.view

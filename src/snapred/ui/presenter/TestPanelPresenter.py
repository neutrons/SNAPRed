from qtpy.QtCore import QObject
from qtpy.QtWidgets import QGridLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.log.logger import snapredLogger
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.workflow.DiffCalWorkflow import DiffCalWorkflow
from snapred.ui.workflow.NormalizationWorkflow import NormalizationWorkflow
from snapred.ui.workflow.ReductionWorkflow import ReductionWorkflow

logger = snapredLogger.getLogger(__name__)


class TestPanelPresenter(QObject):
    def __init__(self, view):
        # `InterfaceController` and `WorkerPool` are singletons:
        #   declaring them as instance attributes, rather than class attributes,
        #   allows singleton reset during testing.
        self.interfaceController = InterfaceController()
        self.worker_pool = WorkerPool()

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

        self.view.tabWidget.addTab(self.reductionWidget, "Reduction")
        self.view.tabWidget.addTab(self.diffractionCalibrationWidget, "Diffraction Calibration")
        self.view.tabWidget.addTab(self.normalizationCalibrationWidget, "Normalization")

    def _addWorkflowWidget(self, widget_):
        layout = QGridLayout()
        widget = QWidget()
        widget.setLayout(layout)
        layout.addWidget(widget_)
        return widget

    @property
    def widget(self):
        return self.view

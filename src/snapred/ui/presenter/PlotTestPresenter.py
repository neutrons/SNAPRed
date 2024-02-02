import json

from qtpy.QtCore import Qt
from qtpy.QtWidgets import QComboBox, QGridLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.view.InitializeCalibrationCheckView import InitializeCalibrationCheckView
from snapred.ui.widget.JsonForm import JsonForm
from snapred.ui.workflow.DiffractionCalibrationCreationWorkflow import DiffractionCalibrationCreationWorkflow
from snapred.ui.workflow.NormalizationCalibrationWorkflow import NormalizationCalibrationWorkflow
from snapred.ui.workflow.ReductionWorkflow import ReductionWorkflow

logger = snapredLogger.getLogger(__name__)


class PlotTestPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        self.view = view

    @property
    def widget(self):
        return self.view

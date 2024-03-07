import json

from qtpy.QtCore import Qt
from qtpy.QtWidgets import QComboBox, QGridLayout, QWidget

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.JsonForm import JsonForm
from snapred.ui.workflow.DiffCalWorkflow import DiffCalWorkflow
from snapred.ui.workflow.NormalizationWorkflow import NormalizationWorkflow
from snapred.ui.workflow.ReductionWorkflow import ReductionWorkflow

logger = snapredLogger.getLogger(__name__)


class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        reductionRequest = SNAPRequest(path="api", payload=None)
        self.apiDict = self.interfaceController.executeRequest(reductionRequest).data

        jsonSchema = json.loads(self.apiDict["config"][""]["runs"])
        self.view = view
        self.jsonForm = JsonForm("Advanced Parameters", jsonSchema=jsonSchema, parent=view)
        self._loadDefaultJsonInput("config//runs", self.jsonForm)
        self.comboSelectionView = BackendRequestView(self.jsonForm, "config//runs", parent=self.view)

        self.diffractionCalibrationWidget = self._createWorkflowWidget(self._createDiffCalWorkflow)
        self.calibrationNormalizationWidget = self._createWorkflowWidget(self._createNormalizationWorkflow)

        self.view.tabWidget.addTab(self.diffractionCalibrationWidget, "Diffraction Calibration")
        self.view.tabWidget.addTab(self.calibrationNormalizationWidget, "Normalization")

        # TODO reenable in Phase 3
        # self.reductionWidth = self._createWorkflowWidget(self._createReductionWorkflow)
        # self.view.tabWidget.addTab(ReductionWorkflow(self.view).widget, "Reduction")

    def _findSchemaForPath(self, path):
        currentVal = self.apiDict
        # TODO: Replace with Config
        subPaths = path.split("/")
        for subpath in subPaths:
            currentVal = currentVal[subpath]
        return currentVal

    def _getSchemaForSelection(self, selection):
        schemaString = self._findSchemaForPath(selection)
        return json.loads(schemaString) if schemaString else {}

    def _loadDefaultJsonInput(self, selection, jsonForm):
        subPaths = selection.split("/")
        subPaths.pop(-1)
        if subPaths[-1] == "":
            subPaths.pop(-1)
        defaultFilePath = "default/request/" + "/".join(subPaths) + "/payload.json"
        if Resource.exists(defaultFilePath):
            defaults = json.loads(Resource.read(defaultFilePath))
            jsonForm.updateData(defaults)
        else:
            logger.warning("No default values for path: {}".format(defaultFilePath))

    def _createWorkflowWidget(self, method):
        layout = QGridLayout()
        widget = QWidget()
        widget.setLayout(layout)

        layout.addWidget(method())
        layout.addWidget(self.calibrationCheckView)
        layout.setAlignment(self.calibrationCheckView, Qt.AlignTop | Qt.AlignHCenter)
        return widget

    def _createDiffCalWorkflow(self):
        path = "calibration/diffraction/request"
        logger.info("Creating workflow for path: {}".format(path))
        jsonSchema = self._getSchemaForSelection(path)
        logger.info("Schema for path: {}".format(jsonSchema))
        newForm = JsonForm(path.split("/")[-1], jsonSchema=jsonSchema, parent=self.view)
        logger.info("Created form for path: {}".format(newForm))
        self._loadDefaultJsonInput(path, newForm)
        logger.info("loaded default json input for path: {}".format(path))
        return DiffCalWorkflow(newForm, parent=self.view).widget

    def _createNormalizationWorkflow(self):
        path = "normalization//request"
        logger.info("Creating workflow for path: {}".format(path))
        jsonSchema = self._getSchemaForSelection(path)
        logger.info("Schema for path: {}".format(jsonSchema))
        newForm = JsonForm(path.split("/")[-1], jsonSchema=jsonSchema, parent=self.view)
        logger.info("Created form for path: {}".format(newForm))
        self._loadDefaultJsonInput(path, newForm)
        logger.info("loaded default json input for path: {}".format(path))
        return NormalizationWorkflow(newForm, parent=self.view).widget

    def _createReductionWorkflow(self):
        # TODO in Phase 3
        pass

    @property
    def widget(self):
        return self.view

import json

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QComboBox, QPushButton

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
from snapred.ui.threading.worker_pool import WorkerPool
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.view.CalibrationReductionRequestView import CalibrationReductionRequestView
from snapred.ui.view.FitMultiplePeaksView import FitMultiplePeaksView
from snapred.ui.view.InitializeCalibrationCheckView import InitializeCalibrationCheckView
from snapred.ui.view.VanadiumFocussedReductionView import VanadiumFocussedReductionView
from snapred.ui.widget.JsonForm import JsonForm
from snapred.ui.workflow.DiffractionCalibrationCreationWorkflow import DiffractionCalibrationCreationWorkflow

logger = snapredLogger.getLogger(__name__)


class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        reductionRequest = SNAPRequest(path="api", payload=None)
        self.apiDict = self.interfaceController.executeRequest(reductionRequest).data

        self.apiComboBox = self.setupApiComboBox(self.apiDict, view)

        jsonSchema = json.loads(self.apiDict["config"][""]["runs"])
        self.view = view
        self.jsonForm = JsonForm("Advanced Parameters", jsonSchema=jsonSchema, parent=view)
        self._loadDefaultJsonInput("config//runs", self.jsonForm)
        self.comboSelectionView = BackendRequestView(self.jsonForm, "config//runs", parent=self.view)
        self.calibrationCheckView = InitializeCalibrationCheckView(parent=self.view)
        self.view.centralWidget.layout().addWidget(self.apiComboBox)
        self.view.centralWidget.layout().addWidget(self.comboSelectionView)
        self.view.centralWidget.layout().setAlignment(self.comboSelectionView, Qt.AlignTop | Qt.AlignHCenter)
        self.view.centralWidget.layout().addWidget(self.calibrationCheckView)
        self.view.centralWidget.layout().setAlignment(self.calibrationCheckView, Qt.AlignTop | Qt.AlignHCenter)
        self.view.adjustSize()

    def _getPaths(self, apiDict):
        paths = []
        for key, value in apiDict.items():
            if type(value) is dict:
                subpaths = self._getPaths(value)
                paths.extend(["{}/{}".format(key, path) for path in subpaths])
            else:
                paths.append(key)
        return paths

    def setupApiComboBox(self, apiDict, parent=None):
        comboBox = QComboBox(parent)
        for path in self._getPaths(apiDict):
            comboBox.addItem(path)

        comboBox.currentIndexChanged.connect(self.handleApiComboSelected)
        return comboBox

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
            logger.warn("No default values for path: {}".format(defaultFilePath))

    def handleApiComboSelected(self, index):  # noqa: ARG002
        selection = self.apiComboBox.currentText()
        jsonSchema = self._getSchemaForSelection(selection)
        # import pdb;pdb.set_trace()
        newForm = JsonForm(selection.split("/")[-1], jsonSchema=jsonSchema, parent=self.view)
        self._loadDefaultJsonInput(selection, newForm)
        if selection.startswith("calibration/reduction"):
            newWidget = DiffractionCalibrationCreationWorkflow(newForm, parent=self.view).widget
        elif selection.startswith("fitMultiplePeaks"):
            newWidget = FitMultiplePeaksView(newForm, parent=self.view)
        elif selection.startswith("vanadiumReduction"):
            newWidget = VanadiumFocussedReductionView(newForm, parent=self.view)
        else:
            newWidget = BackendRequestView(newForm, selection, parent=self.view)

        self.view.centralWidget.layout().replaceWidget(self.comboSelectionView, newWidget)
        self.comboSelectionView.setParent(None)
        del self.jsonForm
        self.jsonForm = newForm
        self.comboSelectionView = newWidget

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def printJsonData(self):
        print(self.jsonForm.collectData())

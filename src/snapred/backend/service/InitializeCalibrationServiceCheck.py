"""
Logical Operations:

Check neutron data exists and load data -> (DataExportService to get Reduction Ingredients)
Check if state exists and create in case it does not exist -> (prompt for new state input)
Iniailize state by calculating the corresponding parameters ->
Finally calculate the grouping-dependent parameters -> (PixelGroupingParameters)
Software should confirm when all operations are complete and execute successfully as "ready to calibrate" status
"""
from PyQt5.QtWidgets import QComboBox, QMessageBox, QInputDialog

from typing import Any, Dict, List

from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class InitializeCalibrationCheck(Service):
    _name = "initializeCalibrationCheck"
    dataFactory = DataFactoryService()
    dataExport = DataExportService()
    calibrationService = CalibrationService()
    request: InitializeStateRequest

    # register the service in ServiceFactory
    def __init__(self):
        super().__init__()
        self.registerPath("initializeCalibrationCheck", self.initializeCalibrationCheck)
        return

    def name(self):
        return self._name

    @FromString
    def initializeCalibrationCheck(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        # check if neutron data exists and load data
        if not runs:
            raise ValueError("List is empty")

        else:
            # list to store states
            states = []

            for run in runs:
                reductionIngredients = self.dataFactory.getReductionIngredients(run.runNumber)
                ipts = reductionIngredients.runConfig.IPTS
                ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)

                # identify the instrument state for measurement
                state = self.dataFactory.getStateConfig(run.runNumber)
                states.append(state)

                # check if state exists and create in case it does not exist
                for state in states:
                    # this boolean will be used to prompt user for new state name input incase it does not exist
                    self.checkStateExists(state)

                # initialize state
                runId = self.request.runId = run.runNumber
                name = self.request.humanReadableName = run.maskFileName  # TODO: Is this correct?
                self.calibrationService.initializeState(runId, name)

                groupingFile = reductionIngredients.reductionState.stateConfig.focusGroups.definition
                # calculate pixel grouping parameters
                try:
                    pixelGroupingParameters = self.calibrationService.calculatePixelGroupingParameters(
                        runs, groupingFile
                    )
                    QMessageBox.information(self, "Ready to Calibrate", "All operations are complete. Ready to calibrate!")
                    return pixelGroupingParameters
                except:
                    raise Exception(QMessageBox.information(self, "Ready to Calibrate", "All operations are complete. Ready to calibrate!"))

    def promptStateName(self):
        state_name, ok_pressed = QInputDialog.getText(self, "State Name", "Enter State Name:")
        if ok_pressed and state_name:
            return state_name
        return None

    def checkStateExists(self, state: StateConfig):
        if state.diffractionCalibrant.name == "":
            state_name = self.promptStateName()
            if state_name is not None:
                state.diffractionCalibrant.name = state_name
                return True
            else:
                return False
        else:
            return True

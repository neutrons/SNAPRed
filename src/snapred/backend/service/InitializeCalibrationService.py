"""
Logical Operations:

Check neutron data exists and load data -> (DataExportService to get Reduction Ingredients)
Check if state exists and create in case it does not exist -> (prompt for new state input)
Iniailize state by calculating the corresponding parameters -> 
Finally calculate the grouping-dependent parameters -> (PixelGroupingParameters)
Software should confirm when all operations are complete and execute successfully as "ready to calibrate" status
"""

from typing import List, Any, Dict

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.service.Service import Service
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.dao.request.InitializeStateRequest import InitializeStateRequest
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton

@Singleton
class InitializeCalibrationService(Service):
    _name = "initialize"
    dataFactory = DataFactoryService()
    dataExport = DataExportService()
    calibrationService = CalibrationService()
    request = InitializeStateRequest()
    pixelGroupingParametersCalculationRecipe = PixelGroupingParametersCalculationRecipe()

    # register the service in ServiceFactory
    def __init__(self):
        super().__init__()
        self.registerPath("", self.initializeCalibration)
        return

    def name(self):
        return self._name
    
    @FromString
    def initializeCalibration(self, runs: List[RunConfig]) -> Dict[Any, Any]:

        # check if neutron data exists and load data
        if not runs:
            raise ValueError("List is empty")

        else:
            for run in runs:
                # reductionIngredients = self.dataFactory.getReductionIngredients(run.runNumber) -> may not need this

                # identify the instrument state for measurement
                states = []
                instrumentState = self.dataExport.getStateConfig(run.runNumber)
                states.append(instrumentState)

                # check if state exists and create in case it does not exist
                if not instrumentState:
                    # TODO: prompt for new state input which might need to be extrapolated to UI popup or text box within the view
                    userInputName = input("Please enter the name of the new state: ") # place holder
                    self.request.humanReadableName = userInputName
                    self.request.runId = run.runNumber
                    
                # if state exists, initialize it
                runId = self.request.runId = run.runNumber
                name = self.request.humanReadableName = run.maskFileName # TODO: Is this correct?
                self.calibrationService.initializeState(runId, name)

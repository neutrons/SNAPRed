from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao import GSASParameters, ParticleBounds

from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients

from snapred.backend.service.SousChef import SousChef
from snapred.backend.service.ReductionService import ReductionService

from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe

from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService

from snapred.meta.Config import Config


from mantid.simpleapi import CropWorkspace, RemovePromptPulse, mtd, Scale, LoadEventNexus, LoadNexusMonitors

##############################################################################################

#  Metadata


MonitorNormID=0
runId="59039"
useLiteMode=True

reductionService = ReductionService()
groups = reductionService.loadAllGroupings(runId, useLiteMode)
# find column group
columnGroup = next((group for group in groups["focusGroups"] if "column" in group.name.lower()), None)
columnGroupWorkspace = next(
    (group for group in groups["groupingWorkspaces"] if "column" in group.lower()), None
)

updatedFarmFresh = FarmFreshIngredients(
    runNumber=runId,
    useLiteMode=useLiteMode,
    focusGroups=[columnGroup], 
)
    
pixelGroup = SousChef().prepPixelGroup(updatedFarmFresh)

dataService = LocalDataService()
instrumentConfig = dataService.readInstrumentConfig()
width = instrumentConfig.width # what should this be?
frequency = instrumentConfig.frequency

detectorState = dataService.readDetectorState(runId)
defaultGroupSliceValue = Config["calibration.parameters.default.groupSliceValue"]
fwhmMultipliers = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
peakTailCoefficient = Config["calibration.parameters.default.peakTailCoefficient"]
gsasParameters = GSASParameters(
    alpha=Config["calibration.parameters.default.alpha"], beta=Config["calibration.parameters.default.beta"]
)
# then calculate the derived values
lambdaLimit = Limit(
    minimum=detectorState.wav - (instrumentConfig.bandwidth / 2) + instrumentConfig.lowWavelengthCrop,
    maximum=detectorState.wav + (instrumentConfig.bandwidth / 2),
)
L = instrumentConfig.L1 + instrumentConfig.L2
tofLimit = Limit(
    minimum=lambdaLimit.minimum * L / dataService.CONVERSION_FACTOR,
    maximum=lambdaLimit.maximum * L / dataService.CONVERSION_FACTOR,
)
particleBounds = ParticleBounds(wavelength=lambdaLimit, tof=tofLimit)



##############################################################################################

# Data

groceryService = GroceryService()
print(groceryService._createNeutronFilename("59039", False))
def loadDataAndMonitors():
    # runWorkspaceData = LoadEventNexus(Filename='/SNS/SNAP/IPTS-28913/shared/lite/SNAP_59039.lite.nxs.h5', OutputWorkspace='tof_all_lite_raw_059039', LoadMonitors=True)
    runWorkspaceData = groceryService.fetchNeutronDataCached(runId, useLiteMode, "")
    monitorWorkspace = LoadNexusMonitors(Filename=groceryService._createNeutronFilename(runId, False), OutputWorkspace=runWorkspaceData["workspace"]+"_monitor") 
    return runWorkspaceData["workspace"]

runWorkspaceName = loadDataAndMonitors()
monitorWorkspaceName = runWorkspaceName + "_monitor"
    

##############################################################################################

# Algorithm

normalizedWorkspaceName = runWorkspaceName + "_normalized"

groceries = {
            "inputWorkspace": runWorkspaceName,
            "groupingWorkspace": columnGroupWorkspace,
            "outputWorkspace": runWorkspaceName,
        }
 
print(pixelGroup.json())
ReductionGroupProcessingRecipe().cook(ReductionGroupProcessingRecipe.Ingredients(pixelGroup=pixelGroup), groceries)


RemovePromptPulse(InputWorkspace=monitorWorkspaceName, OutputWorkspace=monitorWorkspaceName, Width=width, Frequency=frequency)
CropWorkspace(InputWorkspace=monitorWorkspaceName, OutputWorkspace=monitorWorkspaceName, XMin=tofLimit.minimum, XMax=tofLimit.maximum)
normVal = mtd[monitorWorkspaceName].getSpectrum(MonitorNormID).getNumberEvents()


Scale(InputWorkspace=runWorkspaceName, OutputWorkspace=normalizedWorkspaceName, Factor=1/normVal, Operation="Multiply")
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Parameters
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients
from snapred.backend.dao.request import ReductionExportRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.ReductionService import ReductionService
from snapred.meta.Config import Config


runNumber = "59039"
useLiteMode=True
pixelMasks = []
keepUnfocused = True
convertUnitsTo = "dSpacing"
# This is a bitwise flag you can update to say whehter or not you are fine with
# 1. missing calibration
# 2. aritifical normalization
# 3. missing normalization
# 4. ect.
# 
#  e.g. continueFlags = ContinueWarning.Type.MISSING_CALIBRATION | ContinueWarning.Type.MISSING_NORMALIZATION
# continueFlags = ContinueWarning.Type.UNSET  
continueFlags = ContinueWarning.Type.MISSING_NORMALIZATION

artificialNormalizationIngredients = None

# NOTE: Uncomment if you want to perform aritificial normalization
aritificialNormalizationIngredients = ArtificialNormalizationIngredients(
    peakWindowClippingSize = Config["constants.ArtificialNormalization.peakWindowClippingSize"],
    smoothingParameter=0.5,
    decreaseParameter=True,
    lss=True
)


reductionService = ReductionService()
timestamp = reductionService.getUniqueTimestamp()

reductionRequest = ReductionRequest(
    runNumber=runNumber,
    useLiteMode=useLiteMode,
    timestamp=timestamp,
    continueFlags=continueFlags,
    pixelMasks=pixelMasks,
    keepUnfocused=keepUnfocused,
    convertUnitsTo=convertUnitsTo,
    artificialNormalizationIngredients=artificialNormalizationIngredients
)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#  Load the data
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

reductionService.validateReduction(reductionRequest)

# 1. load grouping workspaces from the state folder  TODO: how to init state?
groupings = reductionService.fetchReductionGroupings(reductionRequest)
reductionRequest.focusGroups = groupings["focusGroups"]
# 2. Load Calibration (error out if it doesnt exist, comment out if continue anyway)
# 3. Load Normalization (error out if it doesnt exist, comment out if continue anyway)
# 3. Load the run data (lite or native)  
groceries = reductionService.fetchReductionGroceries(reductionRequest)
groceries["groupingWorkspaces"] = groupings["groupingWorkspaces"]
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#  Load the metdata i.e. ingredients
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# 1. load reduction ingredients

ingredients = reductionService.prepReductionIngredients(reductionRequest, groceries.get("combinedPixelMask"))

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Perform Reduction
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
print(groceries)
data = ReductionRecipe().cook(ingredients, groceries)
record = reductionService._createReductionRecord(reductionRequest, ingredients, data["outputs"])

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#  Save the data
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

saveReductionRequest = ReductionExportRequest(
    record=record
)

reductionService.saveReduction(saveReductionRequest)
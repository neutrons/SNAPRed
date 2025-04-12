from pathlib import Path
import sys
import time

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
from snapred.backend.data.LocalDataService import LocalDataService
## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe as Recipe

# for running through service layer
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.service.ReductionService import ReductionService

from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng, ValueFormatter as wnvf

from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.helpers import (
    createCompatibleMask,
    maskComponentByName
)
from util.IPTS_override import IPTS_override

#############################################################
## If required: override the IPTS search directories:      ##
##   => if you're not using a `dev.yml` this does nothing! ##
with IPTS_override(): # defaults to `Config["IPTS.root"]`  ##
#############################################################

    groceryService = GroceryService()

    #User input ###########################
    runNumber = "46680" #"57482"
    isLite = True


    ### TEST THROUGH THE SERVICE LAYER ###

    # Load the input workspace, so that we can create a _compatible_ mask.
    clerk = GroceryListItem.builder()
    clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
    groceries = GroceryService().fetchGroceryDict(
        groceryDict=clerk.buildDict()
    )

    # Use the "mantid standard" name:
    maskWs = wng.reductionUserPixelMask().numberTag(1).build() # == "MaskWorkspace"
    createCompatibleMask(maskWs, groceries["inputWorkspace"])

    # == mask out several columns ==
    maskComponentByName(maskWs, "Column1")
    maskComponentByName(maskWs, "Column2")
    # maskComponentByName(maskWs, "Column3")
    # maskComponentByName(maskWs, "Column4")
    # maskComponentByName(maskWs, "Column5")
    
    # == mask out everything ==
    # for n in range(1, 7):
    #     maskComponentByName(maskWs, f"Column{n}")

    request = ReductionRequest(
        runNumber = runNumber,
        useLiteMode = isLite,
        timestamp = time.time(),
        pixelMasks = [maskWs]
    )

    service = ReductionService()

    service.reduction(request)



import os
from typing import Any, Dict, List, Tuple

from mantid.simpleapi import CloneWorkspace, mtd

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo as LiteDataAlgo
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import NameBuilder
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    def executeRecipe(
        self,
        filename: str,
        workspace: str,
        loader: str = "",
        instrumentPropertySource=None,
        instrumentSource="",
    ) -> Dict[str, Any]:
        """
        Wraps the fetch groceries algorithm
        inputs:
        - filename -- the path to the file where data is to be fetched
        - workspace -- a string name for the workspace to load data into
        - loader -- the loading algorithm to use, if you think you already know
        - instrumentPropertySource -- (optional) the property to specify the instrument source
        - instrumentSource -- (optional) the source of the instrument for grouping workspaces
        outputs a dictionary with keys
        - "result": true if everything ran correctly
        - "loader": the loader that was used by the algorithm, use it next time (is blank if loading skipped)
        - "workspace": the name of the workspace created in the ADS
        """
        data: Dict[str, Any] = {
            "result": False,
            "loader": "",
        }
        logger.info(f"Fetching data from {filename} into {workspace}")
        algo = FetchAlgo()
        algo.initialize()
        algo.setPropertyValue("Filename", filename)
        algo.setPropertyValue("OutputWorkspace", workspace)
        algo.setPropertyValue("LoaderType", loader)
        if instrumentPropertySource is not None:
            algo.setProperty(str(instrumentPropertySource), instrumentSource)
        try:
            data["result"] = algo.execute()
            data["loader"] = algo.getPropertyValue("LoaderType")
            data["workspace"] = workspace
        except RuntimeError as e:
            raise RuntimeError(str(e).split("\n")[0]) from e
        logger.info(f"Finished fetching {workspace} from {filename}")
        return data

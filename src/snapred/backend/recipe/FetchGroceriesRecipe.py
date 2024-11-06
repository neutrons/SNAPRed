from typing import Any, Dict

from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm as FetchAlgo
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class FetchGroceriesRecipe:
    def __init__(self):
        # NOTE: workaround, we just add an empty host algorithm.
        utensils = Utensils()
        utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def executeRecipe(
        self,
        filename: str,
        workspace: str = "",
        loader: str = "",
        instrumentPropertySource=None,
        instrumentSource="",
        *,
        loaderArgs: str = "",
    ) -> Dict[str, Any]:
        """
        Wraps the fetch groceries algorithm
        inputs:
        - filename -- the path to the file where data is to be fetched
        - workspace -- a string name for the workspace to load data into
        - loader -- the loading algorithm to use, if you think you already know
        - instrumentPropertySource -- (optional) the property to specify the instrument source
        - instrumentSource -- (optional) the source of the instrument for grouping and mask workspaces
        - loaderArgs -- any required keyword arguments for the loader
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
        algo.setPropertyValue("LoaderArgs", loaderArgs)
        if instrumentPropertySource is not None:
            algo.setPropertyValue(str(instrumentPropertySource), instrumentSource)
        try:
            data["result"] = algo.execute()
            data["loader"] = algo.getPropertyValue("LoaderType")
            data["workspace"] = workspace

            if data["loader"] == "LoadEventNexus":
                self.dataService = LocalDataService()
                config = self.dataService.getInstrumentConfig()
                width = config.width
                frequency = config.frequency
                self.mantidSnapper.RemovePromptPulse(
                    "Removing prompt pulse",
                    InputWorkspace=workspace,
                    OutputWorkspace=workspace,
                    Width=width,
                    Frequency=frequency,
                )
                self.mantidSnapper.executeQueue()
        except RuntimeError as e:
            raise RuntimeError(str(e).split("\n")[0]) from e
        logger.info(f"Finished fetching {workspace} from {filename}")
        return data

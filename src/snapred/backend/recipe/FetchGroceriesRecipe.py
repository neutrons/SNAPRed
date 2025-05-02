from typing import Any, Dict

from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FetchGroceriesAlgorithm import FetchGroceriesAlgorithm
from snapred.backend.recipe.algorithm.Utensils import Utensils

logger = snapredLogger.getLogger(__name__)


class FetchGroceriesRecipe:
    def __init__(self):
        # NOTE: workaround, we just add an empty host algorithm.
        utensils = Utensils()
        utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper

    def executeRecipe(
        self,
        filename: str = "",
        workspace: str = "",
        loader: str = "",
        instrumentPropertySource=None,
        instrumentSource="",
        *,
        loaderArgs: str = "{}",
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
        liveDataMode = loader == "LoadLiveDataInterval"
        if liveDataMode:
            logger.info(f"Fetching live data into {workspace}")
        else:
            logger.info(f"Fetching data from {filename} into {workspace}")

        algo = FetchGroceriesAlgorithm()
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
        except (RuntimeError, TypeError) as e:
            # TODO: use `MantidSnapper`!
            name = "FetchGroceriesAlgorithm"
            kwargs = dict(
                FileName=filename,
                OutputWorkspace=workspace,
                LoaderType=loader,
                LoaderArgs=loaderArgs,
                **({str(instrumentPropertySource): instrumentSource} if instrumentPropertySource is not None else {}),
            )
            logger.error(f"Algorithm {name} failed for the following arguments: \n {kwargs}")
            raise AlgorithmException(name, str(e)) from e

        if liveDataMode:
            logger.debug(f"Finished fetching {workspace} from live-data listener")
        else:
            logger.debug(f"Finished fetching {workspace} from {filename}")

        return data

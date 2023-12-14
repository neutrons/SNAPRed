from typing import List

from mantid.api import AlgorithmManager, mtd

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.dao.ingredients.GroceryListBuilder import GroceryListBuilder
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.redantic import list_to_raw_pretty


@Singleton
class GroceryService:
    def __init__(self) -> None:
        self.grocer = FetchGroceriesRecipe()
        pass

    def writeWorkspace(self, path: str, name: WorkspaceName):
        """
        Writes a Mantid Workspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", name)
        saveAlgo.setProperty("Filename", path + name)
        saveAlgo.execute()

    def writeDiffCalTable(self, path: str, name: WorkspaceName):
        """
        Writes a difcal table inside a Mantid TableWorkspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveDiffCal")
        saveAlgo.setPropertyValue("CalibrationWorkspace", name)
        saveAlgo.setPropertyValue("GroupingWorkspace", "")  # TODO
        saveAlgo.setPropertyValue("MaskWorkspace", "")  # TODO
        saveAlgo.setPropertyValue("Filename", path + name)
        saveAlgo.execute()

    def workspaceDoesExist(self, name: WorkspaceName):
        return mtd.doesExist(name)

    def getWorkspaceForName(self, name: WorkspaceName):
        """
        Returns a workspace from Mantid if it exists.
        Abstraction for the Service layer to interact with mantid data.
        Usually we only deal in references as its quicker,
        but sometimes its already in memory due to some previous step.
        """
        if self.workspaceDoesExist(name):
            return mtd[name]
        else:
            self.grocer.fetchDirtyNexusData(GroceryListBuilder().nexus().using(runId).useLiteMode(useLiteMode).build())
            self.grocer.fetchDirtyNexusData(GroceryListItem.makeNexusItem(runId, useLiteMode))

    def readWorkspaceCached(self, runId, name: WorkspaceName):
        """
        Use this in the case you anticipate loading fresh data from disk over and over.
        """
        if self.workspaceDoesExist(name):
            return name
        else:
            self.grocer.fetchCleanNexusData(GroceryListItem.makeNexusItem(runId, useLiteMode))
        pass

    def fetchGroceryList(self, groceryList: List[GroceryListItem]):
        data = self.grocer.executeRecipe(groceryList)
        if data["result"] is not True:
            raise RuntimeError(f"Failure fetching the following grocery list:\n{list_to_raw_pretty(groceryList)}")
        else:
            pass

    def deleteWorkspace(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid.
        Mostly for cleanup at the Service Layer.
        """
        deleteAlgo = AlgorithmManager.create("WashDishes")
        deleteAlgo.setProperty("Workspace", name)
        deleteAlgo.execute()

    def unconditionalDeleteWorkspace(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid, regardless of CIS mode
        Use when a workspace MUST be deleted for proper behavior
        """
        deleteAlgo = AlgorithmManager.create("DeleteWorkspace")
        deleteAlgo.setProperty("Workspace", name)
        deleteAlgo.execute()

from mantid.api import AlgorithmManager, mtd

from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class LocalWorkspaceDataService:
    def __init__(self) -> None:
        pass

    def writeWorkspace(self, path: str, name: WorkspaceName):
        """
        Writes a Mantid Workspace to disk.
        """
        saveAlgo = AlgorithmManager.create("SaveNexus")
        saveAlgo.setProperty("InputWorkspace", name)
        saveAlgo.setProperty("Filename", path + name)
        saveAlgo.execute()

    def getWorkspaceForName(self, name: WorkspaceName):
        """
        Returns a workspace from Mantid if it exists.
        Abstraction for the Service layer to interact with mantid data.
        Usually we only deal in references as its quicker,
        but sometimes its already in memory due to some previous step.
        """
        try:
            return mtd[name]
        except ValueError:
            return None

    def readWorkspaceCached(self, runId, name: WorkspaceName):
        """
        Use this in the case you anticipate loading fresh data from disk over and over.
        """
        pass

    def deleteWorkspace(self, name: WorkspaceName):
        """
        Deletes a workspace from Mantid.
        Mostly for cleanup at the Service Layer.
        """
        if self.getWorkspaceForName(name) is not None:
            deleteWorkspaceAlgo = AlgorithmManager.create("DeleteWorkspace")
            deleteWorkspaceAlgo.setProperty("Workspace", name)
            deleteWorkspaceAlgo.execute()

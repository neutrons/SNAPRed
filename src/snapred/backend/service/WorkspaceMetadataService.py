from typing import List

from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.backend.recipe.WriteWorkspaceMetadata import WriteWorkspaceMetadata
from snapred.backend.service.Service import Service
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class WorkspaceMetadataService(Service):
    def __init__(self):
        super().__init__()
        self.properties = list(WorkspaceMetadata.model_json_schema()["properties"].keys())
        return

    @staticmethod
    def name():
        return "metadata"

    def writeWorkspaceMetadata(self, workspace: WorkspaceName, workspceMetadata: WorkspaceMetadata) -> bool:
        return WriteWorkspaceMetadata().cook(workspceMetadata, {"workspace": workspace})

    def writeMetadataTag(self, workspace: WorkspaceName, logname: str, logvalue: str) -> bool:
        return self.writeMetadataTags(workspace, [logname], [logvalue])

    def writeMetadataTags(self, workspace: WorkspaceName, lognames: List[str], logvalues: List[str]) -> bool:
        metadata = WorkspaceMetadata.model_validate(dict(zip(lognames, logvalues)))
        return self.writeWorkspaceMetadata(workspace, metadata)

    def readWorkspaceMetadata(self, workspace: WorkspaceName) -> WorkspaceMetadata:
        return ReadWorkspaceMetadata().cook({"workspace": workspace})

    def readMetadataTag(self, workspace: WorkspaceName, logname: str) -> str:
        tags = self.readMetadataTags(workspace, [logname])
        if len(tags) == 0:
            return UNSET
        return tags[0]

    def readMetadataTags(self, workspace: WorkspaceName, lognames: List[str]) -> List[str]:
        metadata = self.readWorkspaceMetadata(workspace)
        return [getattr(metadata, logname) for logname in lognames if getattr(metadata, logname) is not UNSET]

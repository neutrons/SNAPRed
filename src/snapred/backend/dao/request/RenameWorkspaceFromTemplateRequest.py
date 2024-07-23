from typing import List

from pydantic import BaseModel, field_validator

from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class RenameWorkspaceFromTemplateRequest(BaseModel, arbitrary_types_allowed=True):
    """
    Rename a list of workspaces according to a template.
    The template must be a formattable string with a placeholder labeled `workspaceName`.
    In the new workspace names, the old workspace name will appear at this label.
    """

    workspaces: List[WorkspaceName]
    renameTemplate: str

    @field_validator("renameTemplate")
    @classmethod
    def renameTemplate_has_workspaceName(cls, v: str) -> str:
        if "{workspaceName}" not in v:
            raise ValueError("Rename template must contain a placeholder labeled 'workspaceName'")
        return v

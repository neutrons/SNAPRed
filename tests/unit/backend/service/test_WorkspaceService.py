from unittest import mock
from unittest.mock import MagicMock

from mantid.simpleapi import CreateSingleValuedWorkspace, GroupWorkspaces, mtd
from snapred.backend.dao.request import (
    RenameWorkspacesFromTemplateRequest,
    ListWorkspacesRequest,
)
from snapred.backend.service.WorkspaceService import WorkspaceService


class TestWorkspaceService:
    def test_name(self):
        assert WorkspaceService.name() == "workspace"

    def test_rename(self):
        mockGroceryService = MagicMock()
        service = WorkspaceService()
        service.groceryService = mockGroceryService
        service.rename('{"oldName": "oldName", "newName": "newName"}')
        mockGroceryService.renameWorkspace.assert_called_once_with("oldName", "newName")

    def test_renameFromTemplate(self):
        mockGroceryService = MagicMock()
        service = WorkspaceService()
        service.groceryService = mockGroceryService
        # must mock out the isGroup function to return false, so that it does not look for child workspaces
        service.groceryService.getWorkspaceForName.return_value = MagicMock(isGroup=MagicMock(return_value=False))
        oldNames = ["old1", "old2"]
        renameTemplate = "{workspaceName}_X"
        newNames = [renameTemplate.format(workspaceName=ws) for ws in oldNames]
        request = RenameWorkspacesFromTemplateRequest(
            workspaces=oldNames,
            renameTemplate=renameTemplate,
        )
        res = service.renameFromTemplate(request.model_dump_json())
        assert newNames == res
        mockGroceryService.renameWorkspaces.assert_called_once_with(oldNames, newNames)

    def test_renameFromTemplate_group(self):
        """
        Create a grouped workspace with one child.
        Make sure that a call to this endpoint will rename BOTH parent and child.
        """
        service = WorkspaceService()
        oldNames = ["parent", "child"]
        # create the needed workspaces
        CreateSingleValuedWorkspace(OutputWorkspace="child")
        GroupWorkspaces(
            InputWorkspaces=["child"],
            OutputWorkspace="parent",
        )
        # create the expected new names
        renameTemplate = "{workspaceName}_X"
        newNames = [renameTemplate.format(workspaceName=ws) for ws in oldNames]
        for old, new in zip(oldNames, newNames):
            assert mtd.doesExist(old)
            assert not mtd.doesExist(new)
        # perform the request
        request = RenameWorkspacesFromTemplateRequest(
            workspaces=["parent"],
            renameTemplate=renameTemplate,
        )
        service.renameFromTemplate(request.model_dump_json())
        # check names changed
        for new in newNames:
            assert not mtd.doesExist(old)
            assert mtd.doesExist(new)

    def test_clear(self):
        mockGroceryService = MagicMock()
        service = WorkspaceService()
        service.groceryService = mockGroceryService
        service.clear('{"exclude": ["name"], "clearCache": false}')
        mockGroceryService.clearADS.assert_called_once_with(["name"], False)

    def test_getResidentWorkspaces(self):
        mockGroceryService = mock.Mock()
        service = WorkspaceService()
        service.groceryService = mockGroceryService
        request = ListWorkspacesRequest(excludeCache=True)
        service.getResidentWorkspaces(request)
        assert mockGroceryService.getResidentWorkspaces.called_once_with(excludeCache=True)
        
        

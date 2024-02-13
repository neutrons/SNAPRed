from unittest.mock import MagicMock

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

    def test_clear(self):
        mockGroceryService = MagicMock()
        service = WorkspaceService()
        service.groceryService = mockGroceryService
        service.clear('{"exclude": ["name"]}')
        mockGroceryService.clearADS.assert_called_once_with(["name"])

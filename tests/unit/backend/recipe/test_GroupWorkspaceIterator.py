import unittest
from unittest.mock import MagicMock, patch

from snapred.backend.recipe.GroupWorkspaceIterator import GroupWorkspaceIterator


class TestGroupWorkspaceIterator(unittest.TestCase):
    @patch(
        "snapred.backend.recipe.GroupWorkspaceIterator.MantidSnapper",
        return_value=MagicMock(
            mtd={
                "groupworkspace": MagicMock(
                    getNumberOfEntries=MagicMock(return_value=3),
                    getItem=MagicMock(
                        side_effect=[
                            MagicMock(getName=MagicMock(return_value="workspace1")),
                            MagicMock(getName=MagicMock(return_value="workspace2")),
                            MagicMock(getName=MagicMock(return_value="workspace3")),
                        ]
                    ),
                )
            }
        ),
    )
    def test_iteration(self, mockMantidSnapper):  # noqa: ARG002
        # Replace 'expectedWorkspaces' with the expected names of workspaces in the grouping workspace
        expectedWorkspaces = ["workspace1", "workspace2", "workspace3"]

        # Iterate through the GroupWorkspaceIterator and compare the obtained workspace names with the expected ones
        actualWorkspaces = []
        for ws_name in GroupWorkspaceIterator("groupworkspace"):
            actualWorkspaces.append(ws_name)

        assert actualWorkspaces == expectedWorkspaces


if __name__ == "__main__":
    unittest.main()

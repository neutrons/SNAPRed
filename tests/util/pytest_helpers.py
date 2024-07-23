import pytest
from mantid.simpleapi import DeleteWorkspaces

# Import required test fixtures at the end of either the main `conftest.py`,
#   or any `conftest.py` at the test-module directory.


def _cleanup_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;
            #   but its implementation body does not. (A really stupid design. :( )
            DeleteWorkspaces(WorkspaceList=_workspaces)
    except RuntimeError:
        pass


@pytest.fixture(scope="function")  # noqa: PT003
def cleanup_workspace_at_exit():
    for f in _cleanup_workspace_at_exit():
        yield f


@pytest.fixture(scope="class")
def cleanup_class_workspace_at_exit():
    for f in _cleanup_workspace_at_exit():
        yield f

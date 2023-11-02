import os
import unittest.mock as mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo

HAVE_MOUNT_SNAP = os.path.exists("/SNS/SNAP/")


@pytest.fixture(autouse=True)
def _setup_teardown():
    """Clear all workspaces before and after tests."""
    workspaces = mtd.getObjectNames()
    for workspace in workspaces:
        try:
            DeleteWorkspace(workspace)
        except ValueError:
            print(f"Workspace {workspace} doesn't exist!")


@pytest.mark.skipif(not HAVE_MOUNT_SNAP, reason="Mount SNAP not available")
def test_LiteDataCreationAlgo_invalid_input():
    """Test how the algorithm handles an invalid input workspace."""
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setPropertyValue("InputWorkspace", "non_existent_ws")
    with pytest.raises(RuntimeError):
        liteDataCreationAlgo.execute()

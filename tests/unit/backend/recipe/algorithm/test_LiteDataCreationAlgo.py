import os
import unittest.mock as mock
from unittest.mock import patch

import numpy as np
import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, Load, mtd
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

@pytest.mark.mount_snap()
@pytest.mark.skipif(not HAVE_MOUNT_SNAP, reason="Mount SNAP not available")
def test_LiteDataCreationAlgo_invalid_input():
    """test how the algorithm handles an invalid input workspace."""
    with pytest.raises(RuntimeError):  # noqa: PT012
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setProperty("InputWorkspace", "non_existent_ws")
        liteDataCreationAlgo.execute()

import os
import unittest.mock as mock

import numpy as np
import pytest

# Mocking external modules
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, Load, mtd
    from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo


# Define the HAVE_MOUNT_SNAP fixture
@pytest.fixture(scope="session")
def HAVE_MOUNT_SNAP():
    return os.path.exists("/SNS/SNAP/")


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
def test_LiteDataCreationAlgo_basic_functionality():
    testWorkspaceFile = "/SNS/SNAP/IPTS-26687/nexus/SNAP_51877.nxs.h5"
    test_ws_name = "test_ws"
    Load(Filename=testWorkspaceFile, OutputWorkspace=test_ws_name)

    assert test_ws_name in mtd.getObjectNames()

    output_ws_name = "output_ws"
    liteDataCreationAlgo = LiteDataCreationAlgo()
    liteDataCreationAlgo.initialize()
    liteDataCreationAlgo.setProperty("InputWorkspace", test_ws_name)
    liteDataCreationAlgo.setProperty("OutputWorkspace", output_ws_name)
    result = liteDataCreationAlgo.execute()

    assert result
    assert output_ws_name in mtd.getObjectNames()
    output_ws = mtd[output_ws_name]

    original_ws = mtd[test_ws_name]
    orig_specs = []
    modified_specs = []
    for idx in range(original_ws.getNumberHistograms()):
        orig_spec = original_ws.getSpectrum(idx)
        orig_specs.append(orig_spec)
        modified_spec = output_ws.getSpectrum(idx)
        modified_specs.append(modified_spec)

    assert len(orig_specs) == len(modified_specs)

    DeleteWorkspace(output_ws_name)


@pytest.mark.mount_snap()
@pytest.mark.skipif(not HAVE_MOUNT_SNAP, reason="Mount SNAP not available")
def test_LiteDataCreationAlgo_invalid_input():
    """test how the algorithm handles an invalid input workspace."""
    with pytest.raises(RuntimeError):  # noqa: PT012
        liteDataCreationAlgo = LiteDataCreationAlgo()
        liteDataCreationAlgo.initialize()
        liteDataCreationAlgo.setProperty("InputWorkspace", "non_existent_ws")
        liteDataCreationAlgo.execute()

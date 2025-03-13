from random import randint
from unittest.mock import MagicMock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, GroupWorkspaces, mtd

from snapred.ui.workflow.WorkflowImplementer import WorkflowImplementer


@pytest.mark.ui
def test_rename_on_iterate_list(qtbot):  # noqa: ARG001
    """
    Test that on iteration, a set of workspaces will be renamed according to the iteration template.
    """
    # setup a list of workspaces to be renamed
    oldNames = ["old1", "old2"]
    for name in oldNames:
        CreateSingleValuedWorkspace(OutputWorkspace=name)

    mockPresenter = MagicMock(iteration=randint(1, 120))

    instance = WorkflowImplementer()
    newNames = [instance.renameTemplate.format(workspaceName=ws, iteration=mockPresenter.iteration) for ws in oldNames]
    instance.outputs = set(oldNames)
    instance.iterate(mockPresenter)
    assert instance.collectedOutputs == set(newNames)


@pytest.mark.ui
def test_rename_on_iterate_group(qtbot):  # noqa: ARG001
    """
    Test that on iteration, a workspace group has all of its members renamed.
    """
    # setup a list of workspaces to be renamed
    oldNames = ["parent", "child1", "child2"]
    CreateSingleValuedWorkspace(OutputWorkspace=oldNames[-1])
    CreateSingleValuedWorkspace(OutputWorkspace=oldNames[-2])
    GroupWorkspaces(InputWorkspaces=oldNames[-2:], OutputWorkspace=oldNames[0])

    mockPresenter = MagicMock(iteration=randint(1, 120))

    instance = WorkflowImplementer()
    newNames = [instance.renameTemplate.format(workspaceName=ws, iteration=mockPresenter.iteration) for ws in oldNames]
    for old, new in zip(oldNames, newNames):
        assert mtd.doesExist(old)
        assert not mtd.doesExist(new)

    instance.outputs = set([oldNames[0]])
    instance.iterate(mockPresenter)
    assert instance.collectedOutputs == set([newNames[0]])
    for old, new in zip(oldNames, newNames):
        assert not mtd.doesExist(old)
        assert mtd.doesExist(new)

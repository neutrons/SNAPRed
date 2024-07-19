import pytest
from pydantic import ValidationError
from snapred.backend.dao.request import RenameWorkspaceFromTemplateRequest


def test_good():
    request = RenameWorkspaceFromTemplateRequest(workspaces=[], renameTemplate="{workspaceName}_X")
    assert request.renameTemplate.format(workspaceName="mike") == "mike_X"


def test_insufficient():
    with pytest.raises(ValidationError) as e:
        RenameWorkspaceFromTemplateRequest(workspaces=[], renameTemplate="workspaceName_X")
    assert "workspaceName" in str(e.value)


def test_bad():
    with pytest.raises(ValidationError) as e:
        RenameWorkspaceFromTemplateRequest(workspaces=[], renameTemplate="{bad_label}_X")
    assert "workspaceName" in str(e.value)

import pytest
from pydantic.error_wrappers import ValidationError
from snapred.backend.dao.WorkspaceMetadata import (
    WorkspaceMetadata,
    diffcal_metadata_state_list,
    normcal_metadata_state_list,
)


def test_literal_bad_diffcal():
    # test failure when adding bad value to diffraction calibration state
    bad = "not in list"
    assert bad not in diffcal_metadata_state_list
    with pytest.raises(ValidationError) as e:
        WorkspaceMetadata(diffcalState=bad)
    assert "diffcalState" in str(e.value)


def test_literal_bad_normcal():
    # test failure when adding bad value to normaliztion state
    bad = "not in list"
    assert bad not in normcal_metadata_state_list
    with pytest.raises(ValidationError) as e:
        WorkspaceMetadata(normalizationState=bad)
    assert "normalizationState" in str(e.value)


def test_literal_good_diffcal():
    for good in diffcal_metadata_state_list:
        try:
            x = WorkspaceMetadata(diffcalState=good)
            assert x.normalizationState == normcal_metadata_state_list[0]
        except ValidationError:
            pytest.fail(f"Unexpected `ValidationError` setting diffcalState to {good}")


def test_literal_good_normcal():
    for good in normcal_metadata_state_list:
        try:
            x = WorkspaceMetadata(normalizationState=good)
            assert x.diffcalState == diffcal_metadata_state_list[0]
        except ValidationError:
            pytest.fail(f"Unexpected `ValidationError` setting normalizationState to {good}")

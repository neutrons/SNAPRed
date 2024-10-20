import pytest
from pydantic import ValidationError
from snapred.backend.dao.WorkspaceMetadata import (
    UNSET,
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
            assert x.normalizationState == UNSET
        except ValidationError:
            pytest.fail(f"Unexpected `ValidationError` setting diffcalState to {good}")


def test_literal_good_normcal():
    for good in normcal_metadata_state_list:
        try:
            x = WorkspaceMetadata(normalizationState=good)
            assert x.diffcalState == UNSET
        except ValidationError:
            pytest.fail(f"Unexpected `ValidationError` setting normalizationState to {good}")


def test_exact_forbid():
    with pytest.raises(ValidationError) as e:
        WorkspaceMetadata(fancyPartyChips="uwu")
    assert "extra" in str(e)

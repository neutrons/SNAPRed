import pytest
from pydantic.error_wrappers import ValidationError
from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
from snapred.meta.mantid.AllowedPeakTypes import allowed_peak_type_list
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt


def test_literal_bad():
    bad = "not in list"
    assert bad not in allowed_peak_type_list
    with pytest.raises(ValidationError) as e:
        CalibrationAssessmentRequest(
            run={"runNumber": "123"},
            workspaces={wngt.DIFFCAL_OUTPUT: ["nope"]},
            focusGroup={"name": "nope", "definition": "nope"},
            calibrantSamplePath="nope",
            useLiteMode=False,
            peakType=bad,
        )
    assert "peakType" in str(e.value)


def test_literal_good():
    for good in allowed_peak_type_list:
        try:
            CalibrationAssessmentRequest(
                run={"runNumber": "123"},
                workspaces={wngt.DIFFCAL_OUTPUT: ["nope"]},
                focusGroup={"name": "nope", "definition": "nope"},
                calibrantSamplePath="nope",
                useLiteMode=False,
                peakType=good,
            )
        except ValidationError:
            pytest.fail("unexpected `ValidationError` during test")

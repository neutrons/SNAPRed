import pytest
from pydantic import ValidationError

from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
from snapred.meta.mantid.AllowedPeakTypes import allowed_peak_type_list
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName as wngn
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt


def test_literal_bad():
    bad = "not in list"
    assert bad not in allowed_peak_type_list
    with pytest.raises(ValidationError) as e:
        CalibrationAssessmentRequest(
            run={"runNumber": "123"},
            workspaces={wngt.DIFFCAL_OUTPUT: [wngn("nope")]},
            focusGroup={"name": "nope", "definition": "nope"},
            calibrantSamplePath="nope",
            useLiteMode=False,
            peakFunction=bad,
            crystalDMin=0.0,
            crystaldMax=10.0,
            nBinsAcrossPeakWidth=10,
        )
    assert "peakFunction" in str(e.value)


def test_literal_good():
    for good in allowed_peak_type_list:
        try:
            CalibrationAssessmentRequest(
                run={"runNumber": "123"},
                workspaces={wngt.DIFFCAL_OUTPUT: [wngn("yup")]},
                focusGroup={"name": "yup", "definition": "yup"},
                calibrantSamplePath="yup",
                useLiteMode=False,
                peakFunction=good,
                crystalDMin=0.0,
                crystalDMax=10.0,
                nBinsAcrossPeakWidth=10,
                maxChiSq=100.0,
            )
        except ValidationError:
            pytest.fail("unexpected `ValidationError` during test")

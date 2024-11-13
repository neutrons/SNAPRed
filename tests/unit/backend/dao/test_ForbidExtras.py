from unittest.mock import sentinel

import pytest
from pydantic import ValidationError
from snapred.backend.dao.ingredients.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

"""
This is just a collection of tests verifying that extra inputs to certain DAOs
are fobidden and generate errors.  Pydantic offers several different ways to
set this, so this test ensures consistent behavior.
Please add more DAO tests to this file as you encounter them.
"""


def test_forbid_exta_diffcalingredients():
    with pytest.raises(ValidationError) as e:
        DiffractionCalibrationIngredients(
            runConfig=sentinel.runConfig,
            pixelGroup=sentinel.pixelGroup,
            groupedPeakLists=sentinel.peakList,
            notPartOfThis=True,
        )
    # there will be errors corresponding to the mandatory arguments
    # we only want to make sure the last one complains about extras
    errors = e.value.errors()
    errors[-1]["loc"] == ["notPartOfThis"]
    errors[-1]["msg"] = "Extra inputs are not permitted"


def test_forbid_exta_diffcalrequest():
    with pytest.raises(ValidationError) as e:
        DiffractionCalibrationRequest(
            runNumber=sentinel.runNumber,
            calibrationSamplePath=sentinel.calibrationSamplePath,
            focusGroup=sentinel.focusGroup,
            useLiteMode=True,
            notPartOfThis=True,
        )
    # there will be errors corresponding to the mandatory arguments
    # we only want to make sure the last one complains about extras
    errors = e.value.errors()
    errors[-1]["loc"] == ["notPartOfThis"]
    errors[-1]["msg"] = "Extra inputs are not permitted"

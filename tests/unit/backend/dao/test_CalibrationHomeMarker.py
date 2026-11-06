import json

import pytest
from pydantic import ValidationError
from util.Config_helpers import Config_override

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationHomeMarker
from snapred.meta.Config import DeployEnvEnum
from snapred.meta.redantic import parse_file_as, write_model_pretty


def test_filename():
    assert CalibrationHomeMarker.filename() == "calibrationHome.json"
    # the filename is not part of the serialized marker
    marker = CalibrationHomeMarker(schemaVersion=1, environment="snapred_prod", createdBy="admin")
    assert set(marker.model_dump()) == {"schemaVersion", "environment", "createdBy", "createdAt"}


def test_filename_from_config():
    with Config_override("instrument.calibration.markerFilename", "otherMarker.json"):
        assert CalibrationHomeMarker.filename() == "otherMarker.json"


def test_environment_values():
    assert DeployEnvEnum.values() == ["snapred_next", "snapred_qa", "snapred_prod", "snapred_exp"]


def test_roundtrip(tmp_path):
    marker = CalibrationHomeMarker(schemaVersion=2, environment="snapred_exp", createdBy="admin")
    assert marker.environment == DeployEnvEnum.EXP
    path = tmp_path / CalibrationHomeMarker.filename()
    write_model_pretty(marker, path)

    raw = json.loads(path.read_text())
    assert set(raw) == {"schemaVersion", "environment", "createdBy", "createdAt"}
    assert raw["environment"] == "snapred_exp"
    # `createdAt` is written as an ISO timestamp
    assert isinstance(raw["createdAt"], str)

    parsed = parse_file_as(CalibrationHomeMarker, path)
    assert parsed.schemaVersion == 2
    assert parsed.environment == DeployEnvEnum.EXP
    assert parsed.createdBy == "admin"
    assert parsed.createdAt == pytest.approx(marker.createdAt, abs=1.0e-6)


def test_invalid_environment():
    with pytest.raises(ValidationError, match="environment"):
        CalibrationHomeMarker(schemaVersion=1, environment="user", createdBy="admin")


def test_extra_forbidden():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CalibrationHomeMarker(schemaVersion=1, environment="snapred_qa", createdBy="admin", notPartOfThis=True)


@pytest.mark.parametrize("missing", ["schemaVersion", "environment", "createdBy"])
def test_required_fields(missing):
    kwargs = {"schemaVersion": 1, "environment": "snapred_qa", "createdBy": "admin"}
    del kwargs[missing]
    with pytest.raises(ValidationError, match=missing):
        CalibrationHomeMarker(**kwargs)

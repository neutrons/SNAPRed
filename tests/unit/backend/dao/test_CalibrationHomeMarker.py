import json

import pytest
from pydantic import ValidationError

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationEnvironment, CalibrationHomeMarker
from snapred.meta.redantic import parse_file_as, write_model_pretty


def test_filename():
    assert CalibrationHomeMarker.FILENAME == "calibrationHome.json"
    # the filename is not part of the serialized marker
    marker = CalibrationHomeMarker(schemaVersion=1, environment="prod", createdBy="admin")
    assert "FILENAME" not in marker.model_dump()


def test_environment_values():
    assert CalibrationEnvironment.values() == ["next", "qa", "prod", "exp"]


def test_roundtrip(tmp_path):
    marker = CalibrationHomeMarker(schemaVersion=2, environment="exp", createdBy="admin")
    assert marker.environment == CalibrationEnvironment.EXP
    path = tmp_path / CalibrationHomeMarker.FILENAME
    write_model_pretty(marker, path)

    raw = json.loads(path.read_text())
    assert set(raw) == {"schemaVersion", "environment", "createdBy", "createdAt"}
    assert raw["environment"] == "exp"
    # `createdAt` is written as an ISO timestamp
    assert isinstance(raw["createdAt"], str)

    parsed = parse_file_as(CalibrationHomeMarker, path)
    assert parsed.schemaVersion == 2
    assert parsed.environment == CalibrationEnvironment.EXP
    assert parsed.createdBy == "admin"
    assert parsed.createdAt == pytest.approx(marker.createdAt, abs=1.0e-6)


def test_invalid_environment():
    with pytest.raises(ValidationError, match="environment"):
        CalibrationHomeMarker(schemaVersion=1, environment="user", createdBy="admin")


def test_extra_forbidden():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CalibrationHomeMarker(schemaVersion=1, environment="qa", createdBy="admin", notPartOfThis=True)


@pytest.mark.parametrize("missing", ["schemaVersion", "environment", "createdBy"])
def test_required_fields(missing):
    kwargs = {"schemaVersion": 1, "environment": "qa", "createdBy": "admin"}
    del kwargs[missing]
    with pytest.raises(ValidationError, match=missing):
        CalibrationHomeMarker(**kwargs)

import json

import pytest

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationEnvironment, CalibrationHomeMarker
from snapred.backend.data.CalibrationRoot import CalibrationRoot
from snapred.meta.redantic import parse_file_as
from snapred.tools.stampCalibrationHome import main, stamp


def readMarker(home):
    return parse_file_as(CalibrationHomeMarker, home / CalibrationHomeMarker.FILENAME)


def test_stamp_empty_directory(tmp_path):
    marker = stamp(tmp_path, "qa", 1, createdBy="admin")
    assert marker.environment == CalibrationEnvironment.QA
    assert readMarker(tmp_path) == marker
    # the directory contains only the marker: no temporary file is left behind
    assert [p.name for p in tmp_path.iterdir()] == [CalibrationHomeMarker.FILENAME]


def test_stamp_creates_directory(tmp_path):
    home = tmp_path / "shared" / "CalibrationExp"
    stamp(home, "exp", 1, createdBy="admin")
    assert readMarker(home).environment == CalibrationEnvironment.EXP


def test_stamp_default_createdBy(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getuser", lambda: "someone")
    assert stamp(tmp_path, "qa", 1).createdBy == "someone"


def test_stamp_not_a_directory(tmp_path):
    path = tmp_path / "file"
    path.write_text("")
    with pytest.raises(NotADirectoryError):
        stamp(path, "qa", 1, force=True)


def test_stamp_refuses_non_empty_directory(tmp_path):
    # never stamp an existing home by accident: e.g. a QA deployment stamping prod as "qa"
    (tmp_path / "Powder").mkdir()
    with pytest.raises(FileExistsError, match="it is not empty"):
        stamp(tmp_path, "qa", 1)
    assert not (tmp_path / CalibrationHomeMarker.FILENAME).exists()


def test_stamp_non_empty_directory_force(tmp_path):
    (tmp_path / "Powder").mkdir()
    stamp(tmp_path, "prod", 1, createdBy="admin", force=True)
    assert readMarker(tmp_path).environment == CalibrationEnvironment.PROD
    assert (tmp_path / "Powder").is_dir()


def test_stamp_refuses_existing_marker(tmp_path):
    stamp(tmp_path, "prod", 1, createdBy="admin")
    with pytest.raises(FileExistsError, match="already stamped: environment 'prod', schema version 1"):
        stamp(tmp_path, "qa", 1)
    assert readMarker(tmp_path).environment == CalibrationEnvironment.PROD


def test_stamp_restamp_force(tmp_path, capsys):
    # e.g. after a manual schema migration
    stamp(tmp_path, "prod", 1, createdBy="admin")
    stamp(tmp_path, "prod", 2, createdBy="admin", force=True)
    assert readMarker(tmp_path).schemaVersion == 2
    assert "Replacing the marker" in capsys.readouterr().out


def test_stamp_refuses_unreadable_marker(tmp_path):
    (tmp_path / CalibrationHomeMarker.FILENAME).write_text("{ corrupt")
    with pytest.raises(FileExistsError, match="unreadable marker file"):
        stamp(tmp_path, "qa", 1)
    stamp(tmp_path, "qa", 1, createdBy="admin", force=True)
    assert readMarker(tmp_path).environment == CalibrationEnvironment.QA


def test_stamp_invalid_environment(tmp_path):
    with pytest.raises(ValueError, match="environment"):
        stamp(tmp_path, "user", 1)
    assert not any(tmp_path.iterdir())


def test_stamped_home_is_accepted(tmp_path):
    stamp(tmp_path, "next", 1, createdBy="admin")
    root = CalibrationRoot(tmp_path, environment="next", schemaVersion=1, requireMarker=True)
    assert root.home == tmp_path


##
## Command line
##


def test_main(tmp_path, capsys):
    assert main(["--home", str(tmp_path), "--env", "exp", "--schema", "1", "--created-by", "admin"]) == 0
    raw = json.loads((tmp_path / CalibrationHomeMarker.FILENAME).read_text())
    assert raw["environment"] == "exp"
    assert raw["schemaVersion"] == 1
    assert raw["createdBy"] == "admin"
    assert "Stamped" in capsys.readouterr().out


def test_main_refused(tmp_path, capsys):
    (tmp_path / "Powder").mkdir()
    assert main(["--home", str(tmp_path), "--env", "qa", "--schema", "1"]) == 1
    assert "it is not empty" in capsys.readouterr().err
    assert not (tmp_path / CalibrationHomeMarker.FILENAME).exists()


def test_main_force(tmp_path):
    (tmp_path / "Powder").mkdir()
    assert main(["--home", str(tmp_path), "--env", "qa", "--schema", "1", "--force"]) == 0
    assert readMarker(tmp_path).environment == CalibrationEnvironment.QA


def test_main_not_a_directory(tmp_path, capsys):
    path = tmp_path / "file"
    path.write_text("")
    assert main(["--home", str(path), "--env", "qa", "--schema", "1"]) == 1
    assert "is not a directory" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args",
    [
        ["--env", "qa", "--schema", "1"],
        ["--home", "x", "--schema", "1"],
        ["--home", "x", "--env", "qa"],
        ["--home", "x", "--env", "user", "--schema", "1"],
        ["--home", "x", "--env", "qa", "--schema", "one"],
    ],
)
def test_main_invalid_arguments(args):
    # environment and schema version are always explicit
    with pytest.raises(SystemExit) as e:
        main(args)
    assert e.value.code == 2

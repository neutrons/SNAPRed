"""
Admin tool: stamp a calibration home with its marker file.

  python -m snapred.tools.stampCalibrationHome --home <path> --env <next|qa|prod|exp> --schema <version>

A deployed SNAPRed refuses to use a calibration home whose marker does not match its configured environment and
schema version.  Existing homes are never stamped automatically: a misconfigured deployment would otherwise
stamp the wrong folder (e.g. a QA deployment stamping prod as "qa"), defeating the check.  For the same reason,
this tool refuses to stamp a non-empty directory, or to replace an existing marker, unless `--force` is given.
"""

import argparse
import getpass
import os
import sys
from pathlib import Path
from typing import List, Optional

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationEnvironment, CalibrationHomeMarker
from snapred.meta.redantic import parse_file_as


def _createArgparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m snapred.tools.stampCalibrationHome",
        description="Stamp a calibration home with its marker file.",
    )
    parser.add_argument("--home", required=True, type=Path, help="the calibration home directory")
    parser.add_argument(
        "--env",
        required=True,
        choices=CalibrationEnvironment.values(),
        help="the deploy environment that the calibration home is intended for",
    )
    parser.add_argument(
        "--schema",
        required=True,
        type=int,
        help="the calibration folder schema version: must match 'instrument.calibration.schemaVersion'",
    )
    parser.add_argument(
        "--created-by", default=None, help="who is stamping the calibration home (default: the current user)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="stamp a non-empty directory, or replace an existing marker (e.g. after a manual schema migration)",
    )
    return parser


def _describe(marker: CalibrationHomeMarker) -> str:
    return f"environment '{marker.environment}', schema version {marker.schemaVersion}, created by '{marker.createdBy}'"


def stamp(
    home: Path, environment: str, schemaVersion: int, createdBy: Optional[str] = None, force: bool = False
) -> CalibrationHomeMarker:
    """
    Write the marker file for `home`.

    Raises `FileExistsError` if `home` is not empty (or already has a marker), unless `force` is set.
    """
    home = Path(home)
    if home.exists() and not home.is_dir():
        raise NotADirectoryError(f"'{home}' is not a directory")

    markerPath = home / CalibrationHomeMarker.FILENAME
    if markerPath.exists():
        try:
            existing = f"it is already stamped: {_describe(parse_file_as(CalibrationHomeMarker, markerPath))}"
        except Exception as e:  # noqa: BLE001
            existing = f"it has an unreadable marker file: {e}"
        if not force:
            raise FileExistsError(f"Refusing to stamp '{home}': {existing}.  Use '--force' to replace the marker.")
        print(f"Replacing the marker of '{home}': {existing}")
    elif home.exists() and any(home.iterdir()):
        if not force:
            raise FileExistsError(
                f"Refusing to stamp '{home}': it is not empty.  "
                "Check that this is the calibration home intended for this environment, then use '--force'."
            )

    marker = CalibrationHomeMarker(
        schemaVersion=schemaVersion,
        environment=environment,
        createdBy=createdBy if createdBy is not None else getpass.getuser(),
    )
    home.mkdir(parents=True, exist_ok=True)
    # write atomically: a partially-written marker would make the home unusable
    tmpPath = markerPath.with_name(f".{markerPath.name}.tmp")
    tmpPath.write_text(marker.model_dump_json(indent=4))
    os.replace(tmpPath, markerPath)
    return marker


def main(args: Optional[List[str]] = None) -> int:
    options = _createArgparser().parse_args(args)
    try:
        marker = stamp(options.home, options.env, options.schema, options.created_by, options.force)
    except (FileExistsError, NotADirectoryError, PermissionError) as e:
        print(e, file=sys.stderr)
        return 1
    print(f"Stamped '{options.home}': {_describe(marker)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

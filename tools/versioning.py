"""
Custom `versioningit` methods for SNAPRed.

`exp` releases are tagged like release candidates, with an optional short name for the experimental feature:
  "v2.4.0exp1", or "v2.4.0exp1-cycle-aware-calib".
"exp" is not a PEP 440 pre-release segment, so hatchling would reject "2.4.0exp1":
these tags are mapped to the PEP 440 local-version form:
  "2.4.0+exp.1", or "2.4.0+exp.1.cycle.aware.calib".

* "exp" remains in the version string, which `python -m snapred --configure` uses to select "snapred_exp.yml".
* The number is kept as its own segment, so that it is compared numerically (otherwise "exp10" < "exp2").
* Name separators are normalized to ".": conda does not allow "-" in a version, and PEP 440
  normalizes "-" and "_" in a local version to "." anyway.
"""

import re
from typing import Any

from versioningit.basics import basic_tag2version

_EXP_VERSION = re.compile(
    r"^(?P<release>\d+(?:\.\d+)*)exp(?P<number>\d+)?(?:[-_.](?P<name>[A-Za-z0-9]+(?:[-_.][A-Za-z0-9]+)*))?$"
)


def tag2version(*, tag: str, params: dict[str, Any]) -> str:
    version = basic_tag2version(tag=tag, params=params)
    if "exp" not in version:
        return version

    match = _EXP_VERSION.match(version)
    if not match:
        raise ValueError(
            f"Invalid exp tag '{tag}': expected 'v<major>.<minor>.<patch>exp<number>[-<name>]', "
            "where <name> contains only letters, digits, and '-', '_', or '.' separators"
        )
    segments = ["exp"]
    if match["number"] is not None:
        segments.append(str(int(match["number"])))
    if match["name"] is not None:
        segments.extend(re.split(r"[-_.]", match["name"].lower()))
    return f"{match['release']}+{'.'.join(segments)}"

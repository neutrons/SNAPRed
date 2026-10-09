import importlib.util
from pathlib import Path

import pytest
from packaging.version import Version

_spec = importlib.util.spec_from_file_location("versioning", Path(__file__).parents[2] / "tools" / "versioning.py")
versioning = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(versioning)


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("v2.4.0exp1", "2.4.0+exp.1"),
        ("v2.4.0exp", "2.4.0+exp"),
        ("v2.4.0exp10", "2.4.0+exp.10"),
        ("v2.4.0exp1-cycle-aware-calib", "2.4.0+exp.1.cycle.aware.calib"),
        ("v2.4.0exp1_Cycle_Aware_Calib", "2.4.0+exp.1.cycle.aware.calib"),
        ("v2.4.0exp1.cycle.aware.calib", "2.4.0+exp.1.cycle.aware.calib"),
        ("v2.4.0exp-cycle-aware-calib", "2.4.0+exp.cycle.aware.calib"),
        ("v2.4.0rc1", "2.4.0rc1"),
        ("v2.4.0", "2.4.0"),
    ],
)
def test_tag2version(tag, expected):
    version = versioning.tag2version(tag=tag, params={})
    assert version == expected
    # must be PEP 440 compliant, and already normalized
    assert str(Version(version)) == version


@pytest.mark.parametrize("tag", ["v2.4.0exp1-", "v2.4.0exp1-read--failover", "v2.4.0exp1-read/failover", "v2.4.0expA"])
def test_tag2version_invalid(tag):
    with pytest.raises(ValueError, match="Invalid exp tag"):
        versioning.tag2version(tag=tag, params={})


def test_tag2version_numeric_ordering():
    assert Version(versioning.tag2version(tag="v2.4.0exp10", params={})) > Version(
        versioning.tag2version(tag="v2.4.0exp2", params={})
    )

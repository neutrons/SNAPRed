##
## Coverage for `SNAPRedGUI`'s save/override/restore of the Mantid default facility and instrument.
##
## These tests exercise `_addLiveDataMantidConfigEntries` / `_restoreLiveDataMantidConfigEntries`
##   directly against a stub, rather than constructing the GUI: the methods only touch
##   `self._mantidConfig` and `self._savedMantidConfigEntries`, so no Qt widgets are required.
##

import pytest
from mantid.kernel import ConfigService

from snapred.meta.Config import Config

# `snapred.ui.main` pulls in the full view layer, which requires a Mantid version matching the
#   `pyproject.toml` pin.  Skip rather than error when the environment is behind.
main = pytest.importorskip(
    "snapred.ui.main",
    reason="'snapred.ui.main' requires the pinned mantid version (see 'pyproject.toml')",
    exc_type=ImportError,
)

DEFAULT_FACILITY_KEY = "default.facility"
DEFAULT_INSTRUMENT_KEY = "default.instrument"

OTHER_FACILITY = "ILL"
OTHER_INSTRUMENT = "IN5"


class _ConfigHolder:
    """Minimal stand-in for `SNAPRedGUI`, exposing only what these two methods use."""

    def __init__(self, mantidConfig):
        self._mantidConfig = mantidConfig
        self._savedMantidConfigEntries = {}


@pytest.fixture
def holder():
    mantidConfig = ConfigService.Instance()
    saved = {key: mantidConfig[key] for key in (DEFAULT_FACILITY_KEY, DEFAULT_INSTRUMENT_KEY)}

    mantidConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    yield _ConfigHolder(mantidConfig)

    mantidConfig.setString(DEFAULT_FACILITY_KEY, saved[DEFAULT_FACILITY_KEY])
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, saved[DEFAULT_INSTRUMENT_KEY])


def test_bothFacilityAndInstrumentAreApplied(holder):
    """Setting the facility alone is not enough -- see `snapred.meta.mantid.LiveDataFacility`."""
    main.SNAPRedGUI._addLiveDataMantidConfigEntries(holder)

    assert holder._mantidConfig[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]
    assert holder._mantidConfig[DEFAULT_INSTRUMENT_KEY] == Config["liveData.instrument.name"]


def test_bothFacilityAndInstrumentAreSaved(holder):
    main.SNAPRedGUI._addLiveDataMantidConfigEntries(holder)

    assert holder._savedMantidConfigEntries[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert holder._savedMantidConfigEntries[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_bothFacilityAndInstrumentAreRestored(holder):
    """The user's Mantid configuration must survive a SNAPRed session unchanged."""
    main.SNAPRedGUI._addLiveDataMantidConfigEntries(holder)
    main.SNAPRedGUI._restoreLiveDataMantidConfigEntries(holder)

    assert holder._mantidConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert holder._mantidConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_addThenRestoreIsIdempotentAcrossRepeatedSessions(holder):
    for _ in range(3):
        main.SNAPRedGUI._addLiveDataMantidConfigEntries(holder)
        main.SNAPRedGUI._restoreLiveDataMantidConfigEntries(holder)

    assert holder._mantidConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert holder._mantidConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT

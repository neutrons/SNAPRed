##
## Regression tests for: "snapred live reduction fails if mantid default instrument is not SNAP".
##
## Root cause: Mantid's `LiveDataAlgorithm::init()` builds the allowed-values list for its
##   `Instrument` property from the *default facility* (`ConfigService.getFacility()`, no argument),
##   restricted to those instruments which have a live-data listener.  When the default facility is
##   not "SNS", that list comes back *empty*, and so `Instrument="SNAP"` is rejected outright with:
##
##     Invalid value for property Instrument (string) from string "SNAP": When setting value of
##     property "Instrument": The value "SNAP" is not in the list of allowed values
##
##   SNAPRed sets `default.facility` only from `SNAPRedGUI.__init__` (see `snapred.ui.main`), so any
##   non-GUI entry point (e.g. `snapwrap`, which imports `snapred.backend` directly) inherits the
##   user's own facility setting and then fails inside `LocalDataService._readLiveData`.
##
## This is a Mantid defect, and the agreed fix is upstream: make that validation dynamic and add an
##   optional `Facility` property to the live-data algorithms.  SNAPRed deliberately does *not* work
##   around it by mutating Mantid's process-wide configuration -- see
##   'docs/source/developer/implementation_notes/mantid_config_ownership.rst'.
##
## These tests characterize the Mantid-side behavior only.  They assert nothing about SNAPRed, so they
##   remain valid both for the present interim workaround (the user sets their own facility) and after
##   the upstream fix lands -- at which point the "rejected" expectations below should start failing,
##   which is the signal that this workaround can be retired.
##

import mantid.simpleapi  # noqa: F401  (import registers `LoadLiveData` with the `AlgorithmFactory`)
import pytest
from mantid.api import AlgorithmManager
from mantid.kernel import ConfigService, amend_config

from snapred.meta.Config import Config

DEFAULT_FACILITY_KEY = "default.facility"
DEFAULT_INSTRUMENT_KEY = "default.instrument"

# A facility which is definitely neither SNS nor HFIR, and which has no live-data listeners at all.
#   Any such facility reproduces the defect: "ILL" is used here only because it is always present
#   in Mantid's `facilities.xml`.
OTHER_FACILITY = "ILL"
OTHER_INSTRUMENT = "IN5"

LIVE_DATA_ALGORITHMS = ("LoadLiveData", "StartLiveData")


@pytest.fixture
def mantidFacilityConfig():
    """Yield the Mantid `ConfigService`, restoring the default facility and instrument afterwards.

    `ConfigService` is a process-wide singleton, so any test which modifies it *must* put it back.
    """
    mantidConfig = ConfigService.Instance()
    saved = {key: mantidConfig[key] for key in (DEFAULT_FACILITY_KEY, DEFAULT_INSTRUMENT_KEY)}

    yield mantidConfig

    # The facility must be restored *before* the instrument: setting the facility resets the
    #   instrument to that facility's default.
    mantidConfig.setString(DEFAULT_FACILITY_KEY, saved[DEFAULT_FACILITY_KEY])
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, saved[DEFAULT_INSTRUMENT_KEY])


def _initializedLiveDataAlgorithm(name: str):
    algorithm = AlgorithmManager.create(name)
    algorithm.initialize()
    return algorithm


def _allowedInstruments(name: str):
    return list(_initializedLiveDataAlgorithm(name).getProperty("Instrument").allowedValues)


@pytest.mark.parametrize("algorithmName", LIVE_DATA_ALGORITHMS)
def test_liveDataInstrumentRejectedWhenDefaultFacilityIsNotSNS(mantidFacilityConfig, algorithmName):
    """The defect itself: a non-SNS default facility makes the SNAP live-data instrument unusable."""
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    liveDataInstrument = Config["liveData.instrument.name"]
    assert liveDataInstrument not in _allowedInstruments(algorithmName)

    algorithm = _initializedLiveDataAlgorithm(algorithmName)
    with pytest.raises(ValueError, match="not in the list of allowed values"):
        algorithm.setProperty("Instrument", liveDataInstrument)


@pytest.mark.parametrize("algorithmName", LIVE_DATA_ALGORITHMS)
def test_liveDataInstrumentAcceptedWhenFacilityAndInstrumentAreSet(mantidFacilityConfig, algorithmName):
    """Setting *both* the facility and the instrument makes the live-data instrument usable again."""
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    facility, instrument = Config["liveData.facility.name"], Config["liveData.instrument.name"]
    with amend_config(facility=facility, instrument=instrument):
        assert mantidFacilityConfig[DEFAULT_FACILITY_KEY] == facility
        assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] == instrument
        assert instrument in _allowedInstruments(algorithmName)

        # No exception: this is the call which fails in the defect report.
        _initializedLiveDataAlgorithm(algorithmName).setProperty("Instrument", instrument)


def test_amendConfigRestoresPreviousFacilityAndInstrument(mantidFacilityConfig):
    """A caller who sets the facility for themselves can put it back afterwards.

    This documents the interim workaround: until the upstream fix lands, a consumer whose default
      facility is not ours can wrap its own calls, and Mantid restores the previous values cleanly.
    """
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    with amend_config(facility=Config["liveData.facility.name"], instrument=Config["liveData.instrument.name"]):
        pass

    assert mantidFacilityConfig[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_settingFacilityAloneDoesNotSetTheInstrument(mantidFacilityConfig):
    """Setting the facility alone never yields the correct default instrument, by either route.

    This is the bug fixed in `SNAPRedGUI._addLiveDataMantidConfigEntries`, which previously set
      `default.facility` and left `default.instrument` wrong.  The two routes differ:

        - `setString(DEFAULT_FACILITY_KEY, ...)` leaves the *previous* instrument in place;
        - `ConfigService.setFacility(...)` resets it to the facility's *first* instrument.

      Neither produces the live-data instrument, so both must be set explicitly.
    """
    facility, instrument = Config["liveData.facility.name"], Config["liveData.instrument.name"]

    # Route 1: `setString` leaves the stale instrument behind.
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, facility)
    assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT
    assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] != instrument

    # Route 2: `setFacility` resets the instrument, but not to the one we need.
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)
    ConfigService.setFacility(facility)
    assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] != OTHER_INSTRUMENT
    assert mantidFacilityConfig[DEFAULT_INSTRUMENT_KEY] != instrument


@pytest.mark.parametrize("algorithmName", LIVE_DATA_ALGORITHMS)
def test_allowedInstrumentsAreSnapshottedAtInitialize(mantidFacilityConfig, algorithmName):
    """The allowed-values list is fixed at `initialize()` and not re-evaluated afterwards.

    This is why adding a `Facility` *property* upstream is not sufficient on its own: property values
      are only set after initialization, by which time the validator is already built.  The upstream
      fix must therefore also make the validation dynamic -- e.g. by moving the check into
      `validateInputs`, where both properties are available.
    """
    instrument = Config["liveData.instrument.name"]

    with amend_config(facility=Config["liveData.facility.name"], instrument=instrument):
        algorithm = _initializedLiveDataAlgorithm(algorithmName)
        algorithm.setProperty("Instrument", instrument)

    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    # The already-initialized algorithm retains both its value and its allowed-values list.
    assert algorithm.getProperty("Instrument").value == instrument
    assert instrument in list(algorithm.getProperty("Instrument").allowedValues)
    algorithm.setProperty("Instrument", instrument)


def test_getInstrumentSearchesAllFacilities(mantidFacilityConfig):
    """`ConfigService.getInstrument(<name>)` is not restricted to the default facility.

    Lookups which name the instrument explicitly -- as `LocalDataService.hasLiveDataConnection` does
      -- therefore keep working under a non-SNS default.  Only the *property validator* is
      facility-bound.
    """
    mantidFacilityConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidFacilityConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    instrumentInfo = ConfigService.getInstrument(Config["liveData.instrument.name"])

    assert instrumentInfo.name() == Config["liveData.instrument.name"]
    assert instrumentInfo.facility().name() == Config["liveData.facility.name"]

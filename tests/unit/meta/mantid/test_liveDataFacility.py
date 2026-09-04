##
## Tests for the temporary live-data facility workaround --
##   `snapred.meta.mantid.liveDataFacility`.  When that workaround is removed, remove these too.
##

import mantid.simpleapi  # noqa: F401  (import registers `LoadLiveData` with the `AlgorithmFactory`)
import pytest
from mantid.api import AlgorithmManager
from mantid.kernel import ConfigService

from snapred.meta.Config import Config
from snapred.meta.mantid.liveDataFacility import liveDataFacility

DEFAULT_FACILITY_KEY = "default.facility"
DEFAULT_INSTRUMENT_KEY = "default.instrument"

# A facility which is neither SNS nor HFIR, and which has no live-data listeners.
OTHER_FACILITY = "ILL"
OTHER_INSTRUMENT = "IN5"


@pytest.fixture
def otherDefaultFacility():
    """Set a non-SNS default facility and instrument, restoring the user's own values afterwards."""
    mantidConfig = ConfigService.Instance()
    saved = {key: mantidConfig[key] for key in (DEFAULT_FACILITY_KEY, DEFAULT_INSTRUMENT_KEY)}

    mantidConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    yield mantidConfig

    mantidConfig.setString(DEFAULT_FACILITY_KEY, saved[DEFAULT_FACILITY_KEY])
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, saved[DEFAULT_INSTRUMENT_KEY])


def test_appliesBothFacilityAndInstrument(otherDefaultFacility):
    """Setting the facility alone never yields the right instrument, so both must be applied."""
    with liveDataFacility():
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]
        assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == Config["liveData.instrument.name"]


def test_restoresTheUsersConfiguration(otherDefaultFacility):
    with liveDataFacility():
        pass

    assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_restoresTheUsersConfigurationAfterAnException(otherDefaultFacility):
    """`ConfigService` is process-wide, so a failed live-data load must not leave our facility behind."""
    with pytest.raises(RuntimeError, match="live-data load failed"):
        with liveDataFacility():
            raise RuntimeError("live-data load failed")

    assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_theScopeIsWhatMakesTheInstrumentAcceptable(otherDefaultFacility):  # noqa: ARG001
    """The reason the workaround exists: `Instrument=SNAP` is only settable inside the scope.

    This pins the Mantid behaviour being worked around.  Once Mantid gains a `Facility` property and
      this workaround is deleted, the "outside the scope" expectation should start failing, which is
      the signal that the deletion is safe.
    """
    instrument = Config["liveData.instrument.name"]

    with liveDataFacility():
        inside = AlgorithmManager.create("LoadLiveData")
        inside.initialize()
        inside.setProperty("Instrument", instrument)  # accepted

    outside = AlgorithmManager.create("LoadLiveData")
    outside.initialize()
    with pytest.raises(ValueError, match="not in the list of allowed values"):
        outside.setProperty("Instrument", instrument)


def test_allowedInstrumentsArePopulatedOnlyInsideTheScope(otherDefaultFacility):  # noqa: ARG001
    """The allowed-values list is what rejects the instrument, and it is fixed at `initialize()`."""
    with liveDataFacility():
        inside = AlgorithmManager.create("LoadLiveData")
        inside.initialize()
        assert Config["liveData.instrument.name"] in list(inside.getProperty("Instrument").allowedValues)

    outside = AlgorithmManager.create("LoadLiveData")
    outside.initialize()
    assert not list(outside.getProperty("Instrument").allowedValues)

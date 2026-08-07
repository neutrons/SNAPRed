from contextlib import nullcontext

import pytest
from mantid.kernel import ConfigService

from snapred.meta.Config import Config
from snapred.meta.mantid.LiveDataFacility import (
    LiveDataFacilityReentranceError,
    assertNoLiveDataFacility,
    liveDataFacility,
    liveDataFacilityActive,
    liveDataFacilityFor,
    needsLiveDataFacility,
)

DEFAULT_FACILITY_KEY = "default.facility"
DEFAULT_INSTRUMENT_KEY = "default.instrument"

# A facility which is neither SNS nor HFIR, and which has no live-data listeners.
OTHER_FACILITY = "ILL"
OTHER_INSTRUMENT = "IN5"


@pytest.fixture
def otherDefaultFacility():
    """Set a non-SNS default facility and instrument, restoring the originals afterwards."""
    mantidConfig = ConfigService.Instance()
    saved = {key: mantidConfig[key] for key in (DEFAULT_FACILITY_KEY, DEFAULT_INSTRUMENT_KEY)}

    mantidConfig.setString(DEFAULT_FACILITY_KEY, OTHER_FACILITY)
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, OTHER_INSTRUMENT)

    yield mantidConfig

    mantidConfig.setString(DEFAULT_FACILITY_KEY, saved[DEFAULT_FACILITY_KEY])
    mantidConfig.setString(DEFAULT_INSTRUMENT_KEY, saved[DEFAULT_INSTRUMENT_KEY])


def test_liveDataFacilitySetsFacilityAndInstrument(otherDefaultFacility):
    with liveDataFacility():
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]
        assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == Config["liveData.instrument.name"]


def test_liveDataFacilityRestoresPreviousSettings(otherDefaultFacility):
    with liveDataFacility():
        pass

    assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_liveDataFacilityRestoresPreviousSettingsAfterAnException(otherDefaultFacility):
    """`ConfigService` is process-wide, so the restore must survive a failing live-data load."""
    with pytest.raises(RuntimeError, match="live-data load failed"):
        with liveDataFacility():
            raise RuntimeError("live-data load failed")

    assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


@pytest.mark.parametrize(
    "algorithmName",
    ["LoadLiveData", "StartLiveData", "MonitorLiveData", "LoadLiveDataInterval"],
)
def test_needsLiveDataFacilityForLiveDataAlgorithms(algorithmName):
    assert needsLiveDataFacility(algorithmName)


@pytest.mark.parametrize("algorithmName", ["LoadEventNexus", "LoadGroupingDefinition", "Rebin", ""])
def test_doesNotNeedLiveDataFacilityForOtherAlgorithms(algorithmName):
    assert not needsLiveDataFacility(algorithmName)


def test_liveDataFacilityForIsANoOpForOtherAlgorithms(otherDefaultFacility):
    scope = liveDataFacilityFor("LoadEventNexus")

    assert isinstance(scope, nullcontext)
    with scope:
        # A non-live-data algorithm must not disturb the user's Mantid configuration at all.
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
        assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_liveDataFacilityForAppliesToLiveDataAlgorithms(otherDefaultFacility):
    with liveDataFacilityFor("LoadLiveData"):
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]
        assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == Config["liveData.instrument.name"]


##
## NON-REENTRANCE
##


@pytest.mark.usefixtures("otherDefaultFacility")
def test_liveDataFacilityIsNotReentrant():
    with liveDataFacility("outer"):
        with pytest.raises(LiveDataFacilityReentranceError, match="cannot be nested"):
            with liveDataFacility("inner"):
                pass


@pytest.mark.usefixtures("otherDefaultFacility")
def test_reentranceErrorNamesTheEnteringContext():
    with liveDataFacility("outer"):
        with pytest.raises(LiveDataFacilityReentranceError, match="entering: LoadLiveData"):
            with liveDataFacility("LoadLiveData"):
                pass


def test_facilityIsStillRestoredAfterAReentranceError(otherDefaultFacility):
    """A rejected inner scope must not disturb the outer scope, nor the eventual restore."""
    with liveDataFacility("outer"):
        with pytest.raises(LiveDataFacilityReentranceError):
            with liveDataFacility("inner"):
                pass
        # The outer scope is still intact.
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]

    assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == OTHER_FACILITY
    assert otherDefaultFacility[DEFAULT_INSTRUMENT_KEY] == OTHER_INSTRUMENT


def test_scopeIsReusableAfterAnException(otherDefaultFacility):
    """The active flag must be cleared on the way out, or the first failure poisons every later call."""
    with pytest.raises(RuntimeError, match="load failed"):
        with liveDataFacility("first"):
            raise RuntimeError("load failed")

    assert not liveDataFacilityActive()

    # A subsequent scope must still work.
    with liveDataFacility("second"):
        assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]


@pytest.mark.usefixtures("otherDefaultFacility")
def test_liveDataFacilityActiveReportsState():
    assert not liveDataFacilityActive()
    with liveDataFacility("scope"):
        assert liveDataFacilityActive()
    assert not liveDataFacilityActive()


def test_assertNoLiveDataFacilityPassesWhenInactive():
    # Must not raise.
    assertNoLiveDataFacility("LoadLiveData")


@pytest.mark.usefixtures("otherDefaultFacility")
def test_liveDataFacilityForIsAlsoNonReentrant():
    with liveDataFacilityFor("LoadLiveData"):
        with pytest.raises(LiveDataFacilityReentranceError):
            with liveDataFacilityFor("LoadLiveDataInterval"):
                pass


def test_ordinaryAlgorithmScopeNestsInsideALiveDataScope(otherDefaultFacility):
    """A live-data algorithm calls ordinary algorithms from inside its own scope; that must be allowed.

    `LoadLiveDataInterval`, for example, calls `CloneWorkspace`, `Plus`, `FilterByTime` and
      `DeleteWorkspace` through `MantidSnapper` from within `PyExec`.
    """
    with liveDataFacilityFor("LoadLiveData"):
        with liveDataFacilityFor("CloneWorkspace"):
            # Still the live-data facility, courtesy of the enclosing scope -- and no error.
            assert otherDefaultFacility[DEFAULT_FACILITY_KEY] == Config["liveData.facility.name"]

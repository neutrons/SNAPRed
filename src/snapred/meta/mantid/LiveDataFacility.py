import threading
from contextlib import AbstractContextManager, contextmanager, nullcontext

from mantid.kernel import amend_config

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)

##
## Mantid's live-data algorithms (`LoadLiveData`, `StartLiveData`, `MonitorLiveData`) build the
##   allowed-values list for their `Instrument` property from the *default facility* -- that is, from
##   `ConfigService.getFacility()` with no argument -- restricted to those instruments which have a
##   live-data listener.  When the default facility is not "SNS", that list comes back *empty*, and so
##   `Instrument="SNAP"` is rejected outright:
##
##     Invalid value for property Instrument (string) from string "SNAP": When setting value of
##     property "Instrument": The value "SNAP" is not in the list of allowed values
##
##   This means SNAPRed cannot reduce SNAP data when the user's Mantid default instrument/facility is
##   set to anything else.
##
## Two properties of this validator determine how it must be worked around:
##
##   1. The allowed-values list is fixed when the algorithm is *initialized*, and is never
##      re-evaluated afterwards.  `AlgorithmManager.create` initializes the algorithm, so the default
##      facility must already be correct at the point of creation -- setting it later has no effect.
##      (This is also why an additional `Facility` *property* on these algorithms would not help:
##      property values are only set after initialization.)
##
##   2. Specifying `Listener` and `Address` explicitly does not avoid the lookup: the algorithm still
##      resolves the listener from the default instrument, and fails with
##      "Attempted to access live listener for <instrument> instrument, which has no listeners."
##
## `ConfigService` is a process-wide singleton, so SNAPRed must not simply set the default facility
##   permanently: doing so at module load would change the facility for the entire Mantid workbench
##   session, and Python offers no dependable module unload.  Instead, the change is made only for as
##   long as it takes to create and run a live-data algorithm, and is then reverted.  Callers are
##   expected to hold this scope under `MantidSnapper._liveDataLock`, so that the interval during
##   which the process-wide default facility is modified stays as short as possible.
##


# Mantid algorithms whose `Instrument` property is validated against the default facility.
LIVE_DATA_ALGORITHMS = frozenset({"LoadLiveData", "StartLiveData", "MonitorLiveData"})


##
## NON-REENTRANCE
##
## While this scope is active, the process-wide default facility is *not* what the user set.  Nesting
##   it would therefore mean an outer live-data call silently running under an inner call's facility,
##   so re-entry is treated as a defect and raised rather than accommodated.  Mantid executes
##   algorithms one at a time, so there is no legitimate reason for these scopes to nest.
##
## Note that nesting live-data calls through `MantidSnapper` already *deadlocks* on
##   `MantidSnapper._liveDataLock`, silently and with no diagnostic.  For the check below to be
##   reached at all, it must therefore be performed *before* that mutex is acquired -- see
##   `MantidSnapper.executeAlgorithm`.
##
_active = False
_activeLock = threading.Lock()


class LiveDataFacilityReentranceError(RuntimeError):
    """Raised when a live-data facility scope is entered while another is already active."""


def liveDataFacilityActive() -> bool:
    """Whether a live-data facility scope is currently in effect anywhere in this process."""
    with _activeLock:
        return _active


def _reentranceError(context: str) -> LiveDataFacilityReentranceError:
    return LiveDataFacilityReentranceError(
        f"A live-data facility scope is already active, and cannot be nested (entering: {context}).\n"
        "  While such a scope is active the Mantid default facility is overridden process-wide,"
        " so nesting would run the outer call under the wrong facility.\n"
        "  This indicates a defect in the calling code: live-data algorithms are expected to run"
        " one at a time."
    )


def assertNoLiveDataFacility(context: str):
    """Fail loudly if a live-data facility scope is already active.

    Call this *before* acquiring any live-data mutex, so that re-entry reports itself instead of
      deadlocking.

    Note that this is an *advisory* check only -- it cannot be atomic with respect to a subsequent
      entry.  `liveDataFacility` performs the authoritative check-and-claim itself; this exists so
      that the common single-threaded nesting mistake is reported before a mutex can swallow it.
    """
    if liveDataFacilityActive():
        raise _reentranceError(context)


def _claimLiveDataFacility(context: str):
    """Atomically claim the live-data facility scope, or raise if it is already claimed.

    The check and the claim *must* occur in a single critical section.  Performing them separately
      allows two threads to both pass the check before either claims, so both then override the
      process-wide Mantid configuration -- and the one which exits last restores the *overridden*
      values rather than the user's, permanently changing the user's default facility.
    """
    global _active

    with _activeLock:
        if _active:
            raise _reentranceError(context)
        _active = True


def _releaseLiveDataFacility():
    global _active

    with _activeLock:
        _active = False


@contextmanager
def liveDataFacility(context: str = "live-data algorithm"):
    """Temporarily make SNAPRed's live-data facility and instrument the Mantid defaults.

    Restores the previous `default.facility` and `default.instrument` on exit, including on error.
      Raises `LiveDataFacilityReentranceError` if such a scope is already active.
    """
    _claimLiveDataFacility(context)
    try:
        facility, instrument = Config["liveData.facility.name"], Config["liveData.instrument.name"]
        logger.debug(f"applying live-data facility '{facility}' / instrument '{instrument}' for {context}")
        with amend_config(facility=facility, instrument=instrument):
            yield
    finally:
        _releaseLiveDataFacility()


def liveDataFacilityFor(algorithmName: str) -> AbstractContextManager:
    """As `liveDataFacility`, but a no-op for algorithms which do not consult the default facility.

    `algorithmName` may name a SNAPRed algorithm which *wraps* a Mantid live-data algorithm (e.g.
      `LoadLiveDataInterval`), in which case the scope must also cover the wrapped child algorithm.
    """
    return liveDataFacility(algorithmName) if needsLiveDataFacility(algorithmName) else nullcontext()


def needsLiveDataFacility(algorithmName: str) -> bool:
    """Whether running `algorithmName` requires SNAPRed's live-data facility to be the Mantid default."""
    # `LoadLiveDataInterval` is a SNAPRed algorithm which creates a `LoadLiveData` child, and whose
    #   `validateInputs` also resolves the instrument via the default facility.
    return algorithmName in LIVE_DATA_ALGORITHMS or algorithmName == "LoadLiveDataInterval"

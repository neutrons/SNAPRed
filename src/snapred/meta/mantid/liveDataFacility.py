##
## TEMPORARY WORKAROUND -- see EWM#15513.
##
## Mantid's `LoadLiveData` validates its `Instrument` property against the *default* facility, so
##   `Instrument="SNAP"` is rejected outright for any user whose default facility is not SNS:
##
##     Invalid value for property Instrument (string) from string "SNAP": When setting value of
##     property "Instrument": The value "SNAP" is not in the list of allowed values
##
##   SNAPRed cannot avoid this by passing an argument, because `LoadLiveData` has no `Facility`
##   property in any released Mantid.  Until it does, the only way for SNAPRed to load live data at all
##   is to make its facility the Mantid default for the duration of the call, and then put the user's
##   setting back.
##
## HOW TO REMOVE THIS FILE
##
##   A Mantid PR adding an optional `Facility` property to the live-data algorithms is in review
##   (mantidproject/mantid#42089).  Once that is released and SNAPRed's pinned Mantid version includes
##   it, replace each `with liveDataFacility():` block with `Facility=Config["liveData.facility.name"]`
##   passed alongside `Instrument`, and delete this module.  The call sites are marked with
##   `TODO (EWM#15513)`.
##
## WHY IT IS SHAPED THIS WAY
##
##   `ConfigService` is a process-wide singleton shared with `mantid_workbench`, so the override has to
##   be as short-lived as possible and must be restored even on failure.  `mantid.kernel.amend_config`
##   does exactly that, including on an exception, and nests correctly if it ever needs to.
##
##   The scope has to cover algorithm *creation* as well as execution: the allowed values for
##   `Instrument` are fixed when the algorithm is initialized, and `AlgorithmManager.create`
##   initializes.  In practice that means wrapping both the `MantidSnapper` call that queues the
##   algorithm and the `executeQueue()` that runs it.
##

from contextlib import AbstractContextManager

from mantid.kernel import amend_config

from snapred.meta.Config import Config


def liveDataFacility() -> AbstractContextManager:
    """Temporarily make SNAPRed's live-data facility and instrument the Mantid defaults.

    Restores the previous `default.facility` and `default.instrument` on exit, including on error.
      Use around any call that reaches Mantid's `LoadLiveData`, covering both the queueing of the
      algorithm and its execution.
    """
    return amend_config(
        facility=Config["liveData.facility.name"],
        instrument=Config["liveData.instrument.name"],
    )

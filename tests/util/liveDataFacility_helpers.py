##
## Shared marker for tests which characterize Mantid's *pre-7.0* live-data `Instrument` validation.
##
## Before Mantid 7.0, `LoadLiveData` / `StartLiveData` restricted their `Instrument` property to the live-data
##   instruments of the *default* facility, fixed at `initialize()`.  Mantid 7.0 includes the upstream fix
##   (mantidproject/mantid#42089): the algorithms gain an optional `Facility` property, and `Instrument` no longer
##   has a static allowed-values list.  The tests marked here pin the old behavior, so they no longer hold.
##
## SNAPRed's `liveDataFacility()` workaround still works under Mantid 7.0 -- it is merely unnecessary -- and its
##   removal is deferred until after the data-source branches land (EWM#15513).  When that workaround is removed,
##   delete the marked tests together with this module.
##

import mantid
import pytest

MANTID_MAJOR_VERSION = int(mantid.__version__.split(".")[0])

requiresPreMantid7LiveDataValidation = pytest.mark.skipif(
    MANTID_MAJOR_VERSION >= 7,
    reason=(
        "characterizes the pre-7.0 live-data `Instrument` validation, fixed upstream in Mantid 7.0 "
        "(mantidproject/mantid#42089); remove along with the `liveDataFacility` workaround (EWM#15513)"
    ),
)

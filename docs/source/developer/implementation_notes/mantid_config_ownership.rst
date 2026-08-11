Mantid ``ConfigService`` ownership
----------------------------------

Mantid's ``ConfigService`` is a process-wide singleton.  When SNAPRed runs inside ``mantid_workbench``
it therefore shares that configuration with the workbench itself and with anything else the user is
doing in that session.  Any SNAPRed change to it is visible everywhere, immediately.

For this reason SNAPRed treats the Mantid configuration as *borrowed*, never *owned*.  Two rules
follow, and they apply to all SNAPRed code:

1. **Never depend on the Mantid default facility or default instrument.**  Resolve both *by name*.
   ``ConfigService.getFacility(<name>)`` and ``ConfigService.getInstrument(<name>)`` search all
   facilities and are unaffected by the user's defaults; the no-argument forms are not.
2. **Wherever an algorithm accepts an instrument, it should also accept a facility.**  An instrument
   name is only meaningful with respect to a facility, so the two belong together.  Either may be
   optional, but it must be possible to override both.  SNAPRed's ``LoadLiveDataInterval`` follows
   this: an empty ``Facility`` means <``liveData.facility.name``>, *not* Mantid's default facility.

Anything SNAPRed does modify -- for example the data-search directories, or the default facility and
instrument in ``SNAPRedGUI`` -- must be saved beforehand and restored afterwards.

The live-data ``Instrument`` validator
======================================

Mantid's live-data algorithms (``LoadLiveData``, ``StartLiveData``, ``MonitorLiveData``) do not follow
rule 1.  They build the allowed-values list for their ``Instrument`` property from the *default
facility* -- ``ConfigService.getFacility()`` with no argument -- restricted to those instruments which
have a live-data listener.  If the default facility is not ``SNS``, that list comes back *empty*, and
``Instrument="SNAP"`` is rejected:

.. code-block:: text

   Invalid value for property Instrument (string) from string "SNAP": When setting value of
   property "Instrument": The value "SNAP" is not in the list of allowed values

So SNAPRed cannot reduce live SNAP data when the user's Mantid default facility is set to anything
else, and there is no argument it can pass to avoid this.

Three properties of that validator are worth recording, since they rule out the obvious workarounds:

1. **The allowed-values list is fixed when the algorithm is initialized**, and never re-evaluated.
   ``AlgorithmManager.create`` initializes, and a second ``initialize()`` is a no-op.  There is no
   Python-exposed way to replace the validator afterwards.
2. **Therefore adding a ``Facility`` property upstream is not sufficient by itself.**  Property values
   are only set *after* initialization, by which time the validator already exists.  The validation
   must also be made *dynamic* -- for instance by moving the check into ``validateInputs``, where both
   properties are available.
3. **Naming the listener explicitly does not avoid the lookup.**  With ``Instrument`` left empty, the
   algorithm resolves the listener from the *default instrument* regardless of what ``Listener`` and
   ``Address`` are set to, failing with "Attempted to access live listener for <instrument>
   instrument, which has no listeners."

Why SNAPRed does not work around this
=====================================

It is technically possible for SNAPRed to override the default facility for the duration of each
live-data call and then restore it.  That approach was implemented, reviewed, and **deliberately
withdrawn**: it adds real complexity, and it is complexity that exists only to compensate for a defect
in code we do not own.  Specifically, it requires SNAPRed to mutate a process-wide singleton that the
workbench is simultaneously reading, which brings its own hazards -- the override must be
non-reentrant, its lifetime has to be reasoned about against Mantid's algorithm-level mutexes, and any
mistake silently corrupts the user's own configuration.

Nor are the alternatives to a scoped override any better.  Setting the facility at SNAPRed **module
load** would change it for the entire workbench session, with no dependable point at which to restore
it, and doing that as an import side effect is an antipattern regardless.  Setting it **only at GUI
startup** is what SNAPRed did historically, and it leaves every non-GUI entry point broken: consumers
which import ``snapred.backend`` directly -- ``SNAPWrap``, scripts, tests -- never construct the GUI.

The agreed direction is therefore to fix the defect at its source: make the live-data validation
dynamic and add an optional ``Facility`` property to those algorithms upstream in Mantid.  A prototype
of that shape validates correctly under a non-SNS default facility, requires no configuration mutation
at all, and produces considerably better diagnostics than "not in the list of allowed values".

Interim workaround
==================

Until that lands, a user whose Mantid default facility is not ``SNS`` must set it themselves before
using live data.  Any of the following is sufficient:

* in ``mantid_workbench``, *File → Settings → General*, set the default facility and instrument;
* in ``~/.mantid/Mantid.user.properties``, set ``default.facility`` and ``default.instrument``;
* from a script, call ``ConfigService.setFacility("SNS")``.

Set the instrument as well as the facility.  Setting the facility alone never yields the right
instrument: ``ConfigService.setString("default.facility", ...)`` leaves the *previous* instrument in
place, while ``ConfigService.setFacility(...)`` resets it to the facility's *first* instrument
(``DAS``, for SNS -- which is not a real instrument).

Note that a *scripted* workaround must restore what it changed. ``mantid.kernel.amend_config`` does
this correctly:

.. code-block:: python

   from mantid.kernel import amend_config

   with amend_config(facility="SNS", instrument="SNAP"):
       ...  # live-data work here

The behaviour described above is pinned by
``tests/unit/backend/data/test_liveDataFacilityConfig.py``.  Those tests assert nothing about SNAPRed,
only about Mantid; when the upstream fix lands, the "rejected" expectations there should begin to
fail, which is the signal that this note and the workaround can be retired.

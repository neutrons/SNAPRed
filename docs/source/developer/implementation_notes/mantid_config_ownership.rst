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
   optional, but it must be possible to override both.  SNAPRed's ``LoadLiveDataInterval`` and
   ``CheckIPTS`` both follow this -- see "Algorithms owned by SNAPRed" below.

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
   instrument, which has no listeners."  This holds for ``Listener`` alone, ``Address`` alone, both
   together, and neither.

Note that only the *validator* is at fault.  ``LiveDataAlgorithm::createLiveListener`` resolves the
instrument with ``ConfigService::Instance().getInstrument(inst_name)``, which searches *all*
facilities, so execution itself is already facility-independent.  The upstream change is therefore
confined to validation, which makes it a smaller and safer PR than it first appears.

How SNAPRed works around this, for now
======================================

The real fix is upstream: make the live-data validation dynamic and add an optional ``Facility``
property to those algorithms in Mantid.  That work is in review as ``mantidproject/mantid#42089``.

SNAPRed cannot simply wait for it, however.  Once the upstream validation is confined to a single
facility -- which is the correct behaviour -- SNAPRed *must* name its facility on every live-data
call, and there is no property to name it with until that PR ships.  So there is no combination of
released Mantid and SNAPRed changes which fixes the defect on its own.

Until then, ``snapred.meta.mantid.liveDataFacility`` makes SNAPRed's facility and instrument the
Mantid defaults for as long as it takes to create and configure a live-data algorithm, and then
restores whatever the user had.  This is **temporary**, and the module documents how to remove it.
Two properties keep it tolerable:

* the override is applied at the point of use, not process-wide, so nothing needs to remember to call
  a SNAPRed setup or teardown hook;
* ``mantid.kernel.amend_config`` restores the previous values on the way out, including on an
  exception, so a failed live-data load cannot leave SNAPRed's facility behind.

The scope must cover algorithm *creation*, because the allowed values for ``Instrument`` are fixed
when the algorithm is initialized.  With ``MantidSnapper`` that means covering both the call which
queues the algorithm and the ``executeQueue()`` which runs it, since an algorithm is created during
each.  It deliberately does **not** cover the repeated ``execute`` calls in
``LoadLiveDataInterval``: released Mantid resolves the listener at execution time through
``ConfigService::getInstrument``, which searches every facility and so does not care what the default
is, and those calls can run for up to <``liveData.dataLoadTimeout``> seconds.  Holding a process-wide
override for that long, in a session shared with the workbench, would be antisocial.

Note what is *not* acceptable.  Setting the facility at SNAPRed **module load** would change it for
the entire workbench session, with no dependable point at which to restore it, and doing that as an
import side effect is an antipattern regardless.  Setting it **only at GUI startup** is what SNAPRed
did historically, and it leaves every non-GUI entry point broken: consumers which import
``snapred.backend`` directly -- ``SNAPWrap``, scripts, tests -- never construct the GUI.

Do not move live-data algorithm construction
============================================

One detail is worth recording here, because an earlier revision of the workaround got it wrong and
the reasoning is not obvious from the code.

``MantidSnapper._liveDataLock`` guards the *execution* of ``LoadLiveData`` and
``LoadLiveDataInterval``, not their *construction*: ``MantidSnapper.executeAlgorithm`` calls
``_createAlgorithm`` before acquiring the mutex, deliberately.  ``LoadLiveData`` has been used as a
stay-resident algorithm, with the instance kept alive across calls to ``execute`` so that its listener
could preload the stream and then keep working against that same stream.  Construction is cheap and
creates no listener -- the listener is created during execution.

The override does need the Mantid configuration to be correct at construction time, and an earlier
revision achieved that by applying it inside ``MantidSnapper`` and moving construction inside the
mutex.  That was justified by an appeal to listener safety, which was simply wrong: constructing the
algorithm does not create a listener.  The current workaround instead wraps the call sites, which
needs no change to ``MantidSnapper`` at all.  Anything which appears to require moving construction
inside these mutexes should be treated as a sign that the approach is wrong.

More generally, Mantid's algorithm lifecycle is awkward -- algorithms are nominally shallow, stateless
wrappers, yet have a managed lifetime -- and ``MantidSnapper``'s algorithm-removal handling has already
been revised more than once.  Expect to have to revisit it again, and be conservative when touching it.

Algorithms owned by SNAPRed
===========================

SNAPRed's own algorithms are, in effect, proposals for general Mantid algorithms, so they should not
reach into SNAPRed's ``Config`` for defaults.  Both ``LoadLiveDataInterval`` and ``CheckIPTS``
therefore declare an optional ``Facility`` alongside ``Instrument``, and neither treats SNAPRed's
configuration as its default.  ``LoadLiveDataInterval`` falls back to *Mantid's* default facility, the
usual Mantid convention; ``CheckIPTS`` falls back to the ORNL facilities (``SNS`` and ``HFIR``), which
preserves the behavior it inherited from Mantid's ``GetIPTS``.  Either way, passing SNAPRed's facility
is the caller's job:

* ``GroceryService._fetchLiveData`` passes ``<liveData.facility.name>`` (forwarded through
  ``FetchGroceriesAlgorithm``);
* ``LocalDataService.getIPTS`` passes ``<facility.name>``.

``CheckIPTS`` is a useful reference for the upstream fix, because it had exactly the same problem: its
``Instrument`` property carried a ``StringListValidator`` built at initialization, which a ``Facility``
property could never influence.  The validator was removed and the check moved into
``validateInputs``.  The cost of that change is the loss of ``allowedValues``, which is what populates
the instrument drop-down in Mantid's *generic* algorithm dialog -- worth weighing upstream, though the
bespoke live-data dialog builds its own list from ``liveListenerInfoList()`` and is unaffected.

Using Mantid's live data directly
=================================

SNAPRed's own live-data calls are covered by the workaround above, so no user action is needed for
them.  Driving Mantid's live-data algorithms *directly* -- from the ``StartLiveData`` dialog, or from a
script of your own -- is still subject to the defect until the upstream fix ships.  In that case, set
the default facility yourself.  Any of the following is sufficient:

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


Possible future direction
=========================

It may be worth SNAPRed exposing a *configuration manager* -- ``SNAPWrap`` in particular could be used
this way -- so that borrowing Mantid configuration has definite enter and exit hooks rather than being
handled ad hoc at each site.  Whether that is warranted depends on how many Mantid settings SNAPRed
really needs to borrow; at present it is only the data-search directories and, in the GUI, the default
facility and instrument.

Mantid ``ConfigService`` ownership
----------------------------------

Mantid's ``ConfigService`` is a process-wide singleton.  When SNAPRed runs inside ``mantid_workbench``
it therefore shares that configuration with the workbench itself and with anything else the user is
doing in that session.  Any SNAPRed change to it is visible everywhere, immediately.

For this reason SNAPRed treats the Mantid configuration as *borrowed*, never *owned*: every entry
that SNAPRed modifies is saved, overridden, and then restored.

Default facility and instrument
===============================

The awkward case is ``default.facility`` / ``default.instrument``.

Mantid's live-data algorithms (``LoadLiveData``, ``StartLiveData``, ``MonitorLiveData``) build the
allowed-values list for their ``Instrument`` property from the *default facility* -- that is, from
``ConfigService.getFacility()`` with no argument -- restricted to those instruments which have a
live-data listener.  If the default facility is not ``SNS``, that list comes back *empty*, and so
``Instrument="SNAP"`` is rejected:

.. code-block:: text

   Invalid value for property Instrument (string) from string "SNAP": When setting value of
   property "Instrument": The value "SNAP" is not in the list of allowed values

SNAPRed consequently cannot reduce SNAP data at all when the user's Mantid default instrument or
facility is set to anything else.

Two properties of this validator constrain any workaround:

1. **The allowed-values list is fixed at algorithm initialization** and never re-evaluated.
   ``AlgorithmManager.create`` initializes the algorithm, so the default facility must *already* be
   correct at the point of creation -- setting it afterwards has no effect.  This is also why simply
   adding a ``Facility`` *property* to these algorithms would not help: property values are only set
   after initialization, by which time the validator is frozen.  An upstream fix would instead have
   to relax the ``StringListValidator`` on ``Instrument`` and perform the check in
   ``validateInputs``, where both properties are available.

2. **Naming the listener explicitly does not avoid the lookup.**  Setting ``Listener`` and
   ``Address`` while leaving ``Instrument`` empty still resolves the listener from the default
   instrument, and fails with "Attempted to access live listener for <instrument> instrument, which
   has no listeners."

Why the change is scoped
========================

Two placements were rejected:

* **At SNAPRed module load.**  This would change the default facility for the entire workbench
  session, and Python offers no dependable module unload, so there would be no correct point at
  which to restore it.  Independently of that, doing resource-affecting work as an import side
  effect is an antipattern.
* **Only at GUI startup.**  This is what SNAPRed did historically (see
  ``SNAPRedGUI._addLiveDataMantidConfigEntries``).  It leaves every non-GUI entry point broken:
  consumers which import ``snapred.backend`` directly -- ``SNAPWrap``, scripts, tests -- never
  construct the GUI, and so inherit the user's own facility setting.

Instead, the default facility and instrument are overridden only for as long as it takes to create
and run a live-data algorithm.  This is implemented by ``snapred.meta.mantid.LiveDataFacility`` and
applied centrally in ``MantidSnapper.executeAlgorithm``, which is the single point through which all
SNAPRed live-data algorithm calls pass.

Because the override *is* still process-wide while it is in effect, it is deliberately entered
*inside* ``MantidSnapper._liveDataLock`` -- the mutex which already serializes ``LoadLiveData`` and
``LoadLiveDataInterval``.  This keeps the interval during which another thread could observe the
modified configuration as short as the live-data call itself.

The scope covers ``execute``, not merely initialization: ``LoadLiveData`` resolves its listener and
address from the facility configuration at execution time, as can be observed from the resulting
connection attempt to the SNAP listener address.

No startup hook is required
===========================

Because the scope is applied at the point of use, there is deliberately *no* SNAPRed startup or
shutdown call for a consumer to remember.  This holds only as long as nothing outside a
``MantidSnapper`` algorithm call depends on the Mantid *default* facility or instrument.  At the time
of writing, the only such dependency is ``LoadLiveDataInterval.validateInputs``, which runs during
``execute`` and is therefore already inside the scope.  Everything else resolves the facility or
instrument *by name* (see "Practical notes" below).

If a future code path needs the default facility outside a live-data algorithm call, that assumption
breaks, and an explicit start/shutdown pair -- which every consumer, including ``SNAPWrap``, would
then have to call -- becomes the fallback.

Non-reentrance
==============

While the scope is active the default facility is not what the user set, so nesting it would mean an
outer live-data call silently running under an inner call's facility.  Re-entry is therefore treated
as a defect: ``liveDataFacility`` raises ``LiveDataFacilityReentranceError`` rather than
accommodating it.  Mantid runs algorithms one at a time, so there is no legitimate reason for these
scopes to nest.

Two details matter here:

* **The check must happen before the live-data mutex is acquired.**  Nesting live-data calls through
  ``MantidSnapper`` already deadlocks on ``_liveDataLock``, silently and with no diagnostic -- both
  ``LoadLiveData`` and ``LoadLiveDataInterval`` map to the same non-reentrant lock.  A check placed
  inside the scope would never be reached.
* **The check must be conditional on the algorithm needing the facility.**  A live-data algorithm
  legitimately calls *ordinary* algorithms through ``MantidSnapper`` from within its own ``PyExec``,
  and hence from inside the scope: ``LoadLiveDataInterval`` calls ``CloneWorkspace``, ``Plus``,
  ``FilterByTime`` and ``DeleteWorkspace``.  Rejecting those would break working code.

Note also that algorithm *creation* now happens inside the mutex rather than before it.  This is
deliberate: creating a second ``LoadLiveData`` instance while one is running risks creating a second
listener, and the previous ordering left a window in which two threads could each construct one
before either acquired the lock.

Practical notes
===============

* Both keys must always be set together.  Setting the facility alone never yields the right
  instrument: ``ConfigService.setString("default.facility", ...)`` leaves the *previous* instrument in
  place, while ``ConfigService.setFacility(...)`` resets it to the facility's *first* instrument
  (``DAS``, for SNS -- which is not a real instrument).
* Look up instruments *by name* wherever possible.  ``ConfigService.getInstrument(<name>)`` and
  ``ConfigService.getFacility(<name>)`` search all facilities and are unaffected by the default, as
  used by ``LocalDataService.hasLiveDataConnection`` and ``CheckIPTS.getValidInstruments``.  Only the
  property *validator* is facility-bound.
* The ``liveData.facility.name`` and ``liveData.instrument.name`` :ref:`Config <applicationyml>`
  entries -- not hard-coded ``"SNS"`` / ``"SNAP"`` -- are what get applied, so that the
  ``TEST_LIVE`` / ``ADARA_FileReader`` mock-listener configuration continues to work for testing.

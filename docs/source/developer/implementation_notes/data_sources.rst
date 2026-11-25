Data sources, and registered neutron data (live reduction under ``livereduce``)
-------------------------------------------------------------------------------

**Status: implemented.**

This design has three parts:

1. **Neutron data sources** (PR 1): a refactoring of the neutron-data fetch in ``GroceryService`` into
   a single system of *data sources*.  This changes no behavior, except that every fetched workspace is
   now tagged with its provenance.  The framework is designed for all of the kinds of data that
   ``GroceryService`` loads, not only neutron data.
2. **Registered neutron data** (PR 2): a new data source, for a workspace that is registered with
   SNAPRed by an external process, such as ``livereduce``.  With part 1 in place, this is one more
   source, plus its registration.
3. **Other data** (PRs 3 and 4): diffraction-calibration tables and masks, and normalization (PR 3);
   then grouping, reduction pixel masks and monitor data (PR 4), are moved onto the same framework.

Motivation
==========

Neutron data sources
~~~~~~~~~~~~~~~~~~~~

The neutron data for a run can now come from:

* the native NeXus file on disk;
* the lite-mode file on disk;
* SNAPRed's own live-data listener;
* conversion of native data to lite mode, in this process.  The converted data is then (optionally)
  exported to disk, where it becomes the lite-mode file for later fetches.

The choice between these is spread over the nested ``_fetchNeutronDataLite`` -> ``_fetchNeutronDataNative``
-> ``_fetchNeutronDataSingleUse`` calls, a ``missingDataHandler`` callback, and an ad-hoc
``fromLiveData`` flag, which decides whether converted data is exported.  The ``_loadedRuns`` cache is
interleaved with all of these.  Nothing records which of the sources a workspace actually came from.
Adding a further source to this structure would make it harder to follow, and ``GroceryService`` is
already very large.

Registered neutron data
~~~~~~~~~~~~~~~~~~~~~~~

There is a need to run reduction -- with all of the additions that SNAPWrap makes to it -- from within
Mantid's live-reduction framework, `livereduce <https://github.com/mantidproject/livereduce>`_.

``livereduce`` is a headless daemon, run by ``systemd`` as ``snsdata`` on the beamline.  It calls
``StartLiveData`` itself (``AccumulationMethod="Add"``, ``FromStartOfRun=True``,
``UpdateEvery`` from its configuration, 30 s by default), and runs two instrument-provided scripts from
``/SNS/SNAP/shared/livereduce``:

* ``reduce_SNAP_live_proc.py``: run on each incoming chunk (``input``), writing ``output``, which is
  added to an accumulation workspace;
* ``reduce_SNAP_live_post_proc.py``: run on each update, with the *accumulated* workspace as ``input``.

The accumulated data is therefore created and owned by ``livereduce``, not by SNAPRed.  How that data
is produced (the processing script) is outside the scope of SNAPRed.

Decisions
=========

1. **One system describes where data comes from.**  Each source is an object that declares the kind
   of data and the resolution (native or lite) that it provides.  The sources of each resolution are
   held in a registry of their own.  A source that derives its data from other data obtains that data
   itself, from the registry that provides it.  The order in which sources are tried is configuration,
   not code.

2. **Each source provides exactly one output.**  Where two outputs come from the same file (a
   diffraction-calibration table and its mask), there are two sources, which share an implementation.

3. **Every fetched workspace records its provenance**: the chain of sources that produced it.

4. **Reduction is not aware of registered data.**  ``ReductionRequest`` and ``GroceryListItem`` are
   *not* extended with an input-workspace field.  The concern is not that a workspace comes from
   outside SNAPRed, but that it would be *unmanaged*: any request could name any workspace, as a
   second way of initializing run data, with no record of where that data came from and no SNAPRed
   control of its lifecycle.  It would also bloat the request.

5. **An external workspace is formally *registered* with SNAPRed as the data source for a run
   number.**  Registration goes through an API endpoint (a ``SNAPRequest``), so that it passes
   through the interface layer like any other request.  A reduction of that run number then proceeds
   exactly as it does now; only the source of the neutron data differs.

   By registering a workspace, the caller hands it over to SNAPRed as the data for that run -- in
   spirit.  This is a compromise: SNAPRed cannot guarantee that the caller does not go on to modify or
   delete the workspace (``livereduce`` itself adds to it on every update).  SNAPRed limits the effect
   of this: it validates the registration on each fetch, works only on a clone, never caches the data,
   and records the origin of the data in its provenance.

6. **Output is not saved on each update.**  Whether, and when, to save is a decision for the caller
   (SNAPWrap or the post-processing script), and does not involve SNAPRed.

Design: neutron data sources
============================

The sources, the registries, the resolvers and the registration owner go into a new package,
``snapred.backend.data.source``.  ``GroceryService`` keeps its public fetch methods
(``fetchNeutronDataSingleUse``, ``fetchNeutronDataCached``, ``clearLiveDataCache``, ...), and delegates to
``NeutronDataSources``.

Sources
~~~~~~~

.. code-block:: python

    class DataSource(ABC):
        kind: DataSourceKind      # nexusFile, liteFile, live, registered, liteConversion, ...
        provides: Resolution      # native or lite
        product: Product          # neutron, diffcalTable, diffcalMask, normalization,
                                  #   grouping, reductionPixelMask, monitor
        cacheable: bool = True

        def available(self, item: GroceryListItem) -> bool: ...
        def exclusiveFor(self, item: GroceryListItem) -> bool: ...   # False, except as below
        def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData: ...

    class DataSourceRegistry:
        product: Product
        resolution: Resolution    # every source in the registry provides this product, at this resolution
        def ordered(self) -> list[DataSource]: ...   # in the order set by `dataSources.<product>.<resolution>`

``SourcedData`` holds the workspace name, the loader that was used, its resolution, the provenance chain,
and, for derived data, the data that it was derived from (``derivedFrom``).

``NeutronDataSources`` builds the two registries, a resolver for each, and the shared cache.
``GroceryService`` holds only that object (``neutronData``), and delegates its neutron-data fetches to
it.  The registries are:

=========================  ===========  ==========  =========
native registry            provides     cacheable   exclusive
=========================  ===========  ==========  =========
``FileSource`` (native)    native       yes
``LiveListenerSource``     native       no [#]_
``RegisteredSource``       native       **no**      **yes**
=========================  ===========  ==========  =========

=========================  ===========  ==========  ==========================================
lite registry              provides     cacheable   exclusive
=========================  ===========  ==========  ==========================================
``FileSource`` (lite)      lite         yes
``LiteConversionSource``   lite         yes         when a native source is exclusive
=========================  ===========  ==========  ==========================================

.. [#] As now: live data has a cache entry, which ``clearLiveDataCache`` removes, but no raw workspace,
   so it is never reused.

* ``FileSource`` is one implementation for both resolutions: its kind is ``nexusFile`` or ``liteFile``.
  It is not available when the item requests live data
  (``item.liveDataArgs`` is set), as now.
* ``LiveListenerSource`` keeps all of its existing behavior: the run-status checks, the run-number
  checks, the ``LiveDataState`` exceptions, and the different errors for an explicit live-data request
  and for the live-data fallback.
* ``LiteConversionSource`` depends on the native sources, so it is not their neighbor: it is given the
  *native registry*, and obtains its native data through a ``NeutronDataResolver`` of its own, built
  from that registry and the shared cache.  It clones the native data and converts the clone to lite
  mode.  It exports the converted data to disk **only when the native data came from** ``nexusFile`` (or has no recorded provenance:
  a cached workspace that was not loaded through a data source).  This replaces the
  ``fromLiveData`` flag, and means that neither live nor registered data is ever exported: a lite-mode
  file of a partially-acquired run would otherwise be loaded later, by every file-based reduction of
  that run, as though it were the complete data.

Resolver
~~~~~~~~

A ``NeutronDataResolver`` produces the data for an item from one registry, so at one resolution.
``NeutronDataSources`` has a resolver for each registry, and chooses one by the item's ``useLiteMode``.

1. If an *exclusive* source is available for the item, it is the only source that may be used.  For a
   lite-mode request, an exclusive native source must also exclude the lite-mode file: the lite
   registry treats ``LiteConversionSource`` as exclusive when its native registry has an exclusive
   source available for the item, so that the registered data is converted.
2. Otherwise, the sources in the registry are tried in the configured order.  The first available
   source is used.
3. If no source is available, the resolver raises an error.

The resolvers do not resolve one resolution from another: a deriving source (``LiteConversionSource``)
does that itself, through its own resolver.

The order is set in ``application.yml``:

.. code-block:: yaml

    dataSources:
      neutron:
        native: [registered, nexusFile, live]
        lite: [liteFile, liteConversion]
      diffcalTable:
        native: [calibrationFile, calibrationRecord]
        lite: [calibrationFile, calibrationRecord]
      ...

Cache
~~~~~

The ``_loadedRuns`` cache moves out of ``GroceryService`` into its own object, ``NeutronDataCache``.  It
keeps its current behavior: a raw workspace per run number and resolution, and numbered copies for each
cached fetch, including the native data of a lite-mode conversion (its ``derivedFrom``), which is
cached at its own resolution.  Each resolver checks the cache before any source; the cache is shared, so
the conversion's native resolver uses cached native data.  Data
from a source that is not cacheable is given a cache entry without a raw workspace, so that it is never
reused.  A cache hit does not add to the provenance: a copy of a cached workspace has the provenance of
the original.

Provenance metadata
~~~~~~~~~~~~~~~~~~~

Add a ``dataSource`` field to ``WorkspaceMetadata`` (``snapred.backend.dao.WorkspaceMetadata``).  Its value
is the provenance chain, from the original source to the final one, for example:

* ``nexusFile``
* ``liteFile``
* ``nexusFile>liteConversion``
* ``live>liteConversion``
* ``registered>liteConversion``

The value is ``unset`` for workspaces created before this field existed.  The resolver writes it on
*every* neutron-data fetch -- otherwise a missing tag would be ambiguous.  As workspace metadata is held
in the logs, it propagates through cloning to the reduction output.  ``ReductionService`` already reads
this metadata, so ``dataSource`` could later be carried into the ``ReductionRecord`` as well.

A single chain value is used, rather than separate fields for the original and the final source, so
that it remains correct if more transforming sources are added.

Design: other data
==================

The same framework applies to the other kinds of data that ``GroceryService`` loads.  Each of these was
a separate method, with its own check for a resident workspace, its own load, and its own cache.  This is
done in two steps: first the calibration and normalization data, then the grouping, the reduction pixel
mask and the monitor.

Calibration and normalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``CalibrationDataSources`` builds the registries, the resolvers and the caches of three products, at each
resolution.  ``GroceryService`` holds it as ``calibrationData``.

=====================  =========================================  =================================
product                sources                                    cache
=====================  =========================================  =================================
diffCal table          ``calibrationFile`` (exclusive),            resident in the ADS, by name
                       ``calibrationRecord``
diffCal mask           ``calibrationFile`` (exclusive),            resident in the ADS, by name
                       ``calibrationRecord``
normalization          ``normalizationRecord``                    one entry: the last one loaded
=====================  =========================================  =================================

* **Named sources:** where the name of the workspace depends on where the data comes from (the calibration
  record, or the file that ``item.diffCalFilePath`` names), the source names it.  So the generic
  ``DataResolver`` chooses the source first, asks it for the name, and then checks the cache: the reverse
  of the neutron-data resolver, whose names do not depend on the source.  A source may have no such data
  for an item (a calibration record without a mask): the result is then ``None``.
* **One output per source:** the diffCal table and mask sources share an implementation, which runs
  ``LoadCalibrationWorkspaces`` with only the one output requested.  So a ``diffcal_table`` fetch no longer
  also loads the mask, and applying a diffraction calibration in ``_processNeutronDataCopy`` loads only the
  table.
* **Native and lite share an implementation:** the native and lite sources of each product are one
  implementation, parameterized by resolution, which only selects which file the lookup finds.
* **Instrument donor:** the calibration sources need an instrument donor.  The donor cache moves out of
  ``GroceryService`` into ``InstrumentDonors``, which the sources use directly.
* **Processing step:** a normalization that is loaded (not reused) is then processed as neutron data, by
  ``GroceryService`` (``_processNeutronDataCopy``): the processing is not part of the source.
* **Provenance:** the mask and the normalization are tagged with ``dataSource``.  A table workspace has no
  logs, so its provenance is only returned, and is unknown when the table is reused.  The existing
  ``diffcalState`` and ``normalizationState`` tags are unchanged: they record what was *applied* to a
  sample workspace, not where a workspace came from.
* **Boundary:** a source decides how a product is brought into the ADS.  *Which* file and version contain
  it is still decided by the indexer and ``LocalDataService`` (including, on ``exp``, the read-only
  calibration failover), below the source layer.
* ``fetchDefaultDiffCalTable`` is not a source: it generates a default table under its own name, and is
  never a fallback for an indexed table.

Grouping, reduction pixel mask and monitor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ReductionDataSources`` builds the registries, the resolvers and the caches of three more products.
``GroceryService`` holds it as ``reductionData``.  All three are named sources, resolved by the generic
``DataResolver``.

=====================  ===========================  ==========  ===========================================
product                sources                      resolution  cache
=====================  ===========================  ==========  ===========================================
grouping               ``groupingFile``             both        by grouping scheme and state
                                                                (``GroupingDataCache``)
reduction pixel mask   ``reductionRecord``          both        resident in the ADS, by name
monitor                ``nexusFile``                native      resident in the ADS, by name
=====================  ===========================  ==========  ===========================================

.. code-block:: yaml

    dataSources:
      ...
      grouping:
        native: [groupingFile]
        lite: [groupingFile]
      reductionPixelMask:
        native: [reductionRecord]
        lite: [reductionRecord]
      monitor:
        native: [nexusFile]

* **Cache lookup by item:** a grouping workspace is reused for every run of the same state, under the
  name that it was first loaded with, which names the run it was loaded for.  So the cache cannot be
  checked by the name that the source gives: the cache decides the name.  The cache interface becomes
  ``lookup(item, proposedName) -> (name, cached)`` and ``record(item, name)``.  ``DataResolver`` passes
  the name that the source gives, and uses the name that the cache returns, whether to reuse or to load.

  * ``ResidentDataCache`` and ``SingleEntryDataCache`` ignore the item: they return ``proposedName``, and
    whether it is resident.  The calibration and normalization fetches are unchanged.
  * ``GroupingDataCache`` is keyed by grouping scheme, state and resolution.  It chooses the name
    recorded for the key, or else ``proposedName``.  Then it makes the entry agree with the ADS, as
    ``_updateGroupingCacheFromADS`` does now: if that workspace is resident and has no entry, it is
    recorded, and if it has an entry but is no longer resident, the entry is dropped.  It returns the
    chosen name, and whether the key now has an entry.

  ``_loadedGroupings``, ``_updateGroupingCacheFromADS`` and ``rebuildGroupingCache`` move into
  ``GroupingDataCache``; ``GroceryService.rebuildGroupingCache`` and ``getCachedWorkspaces`` delegate to
  it.  Nothing outside ``GroceryService`` uses the grouping cache, except the tests and their
  ``InstaEats`` stand-in, which are updated.
* **A grouping that is no longer resident is reloaded under the name that was cached for the state**, as
  now: the name is chosen before the entry is checked against the ADS.
* **Which file:** ``groupingFile`` loads the file that the state's grouping map names for the scheme, or
  ``instrument.lite.map.file`` for the ``Lite`` scheme, as now.  That choice stays in ``LocalDataService``
  and ``Config``, below the source layer, as for the calibration files.
* **Instrument donor:** the grouping and pixel-mask sources use ``InstrumentDonors``, unless the item sets
  ``instrumentPropertySource``, as now.
* **One output per source:** the pixel-mask source runs ``LoadCalibrationWorkspaces`` with only the mask
  requested, as now.
* **Monitor is native only:** a monitor has no lite-mode form.  ``fetchMonitorWorkspace`` resolves a
  copy of the item with ``useLiteMode`` set to ``False``, using the native resolver, and there is no lite
  registry.  A monitor that is in the ADS is reused, as now: until now that was only because
  ``FetchGroceriesAlgorithm`` skips the load of an existing workspace.  It is not one of the service's
  cached workspaces, as now.
* **Missing IPTS directory:** the monitor source is always available, and its ``load`` raises the
  existing ``Cannot find IPTS directory`` error: ``ReductionService`` matches that message.
* **Provenance:** the grouping, the pixel mask and the monitor have logs, so all three are tagged with
  ``dataSource``.  ``nexusFile`` is the same kind as for neutron data: a kind records where data came
  from, not which product it is.
* **Not sources:** ``fetchCompatiblePixelMask`` generates a blank mask, so like
  ``fetchDefaultDiffCalTable`` it is not a source.  The public return values of
  ``fetchGroupingDefinition``, ``fetchReductionPixelMask`` and ``fetchMonitorWorkspace`` are unchanged.

Design: registered neutron data
===============================

Registration owner
~~~~~~~~~~~~~~~~~~

A dedicated ``@Singleton``, ``NeutronDataRegistrations``, owns the run-number -> registered-workspace
associations, and their validation.  It is used by ``RegisteredSource`` and by the run-metadata lookup (below); it is exposed
through the registration endpoints.  No parameters are added to existing classes to support it.

* Registration should be performed from the *post-processing* script: only that script sees the
  accumulation workspace (its name is generated by ``livereduce`` as
  ``mtd.unique_name(prefix="accumulation_")``).  Re-registering on every update must be harmless.
* The endpoints are ``workspace/registerNeutronData`` (``RegisterNeutronDataRequest``: ``runNumber``,
  ``workspace``) and ``workspace/unregisterNeutronData`` (``UnregisterNeutronDataRequest``: ``runNumber``).
* Registration fails if the workspace does not exist, or if its ``run_number`` log is set to a different
  run.  The registrations of different run numbers are independent: the same workspace may be registered
  under several run numbers, and registering it under one doesn't remove the others.  Removing a
  registration never deletes the workspace.  Removing a registration that does not exist is harmless.
* **Data without a run number** (the ``run_number`` log is missing, or 0: live data between runs, or
  before a run has been set up) is registered under ``ReservedRunNumber.LIVE`` (``999999``).  Any data
  can be registered under it.  It is numeric and later than any real run, so the run-number rules
  (validation, ``Indexer`` applicability) need no special case: the latest calibration and normalization
  for the state apply, where the state is generated from the data's PV logs.  When a run starts, the
  registering process registers the workspace under the run number too.  Its ``LIVE`` registration is kept
  until the registering process removes it.
* **Validate the registration on each lookup:** if the workspace no longer exists (for example,
  ``livereduce``'s memory watchdog has restarted the daemon and cleared the ADS), raise an error that asks
  for the data to be registered again, or for the registration to be removed.  The registration is kept.
  A registration is removed only explicitly, or replaced by another registration for the same run: the data
  for a registered run never silently comes from another source, which could hide an error in the process
  that registered it.
* **The ``run_number`` log is checked once**, when the data is registered under a real run number, and
  not on lookup.  Run changes and accumulation are the business of the registering process, which
  registers its data again on each update.  The clone that ``RegisteredSource`` makes is given the run
  number of the registration.

``RegisteredSource``
~~~~~~~~~~~~~~~~~~~~

* **Clone** the registered workspace, and operate only on the clone: event filtering, prompt-pulse
  removal and ``ApplyDiffCal`` all modify their workspace in place.
* **Never delete, rename or otherwise modify** the registered workspace itself; exclude it from
  ``clearADS`` and from the cache-clearing paths.
* It is **not cacheable**: the registered workspace grows between updates, so a cached clone would be
  stale.
* It is **exclusive**: while a run is registered, its data never comes from a file, and a lite-mode
  request is converted from the registered data.  A lite-mode file on disk could only be from an
  earlier, partial, acquisition.
* **A live-data request for a registered run is an error.**  If the item requests live data
  (``item.liveDataArgs`` is set) for a run that is registered, the fetch raises an error that asks for
  the registration to be removed first.  SNAPRed does not silently choose either source.

Run metadata
~~~~~~~~~~~~

State and instrument parameters are derived from ``LocalDataService.readRunMetadata(runNumber)``.  For a
live run there is no PV file yet, so this falls back to ``readLiveMetadata``, which opens its *own*
listener connection, and the result is cached by run number.  For a registered run, the metadata should
instead be read from the registered workspace's logs (as ``LocalDataService.stateIdFromWorkspace``
already does, using ``RunMetadata.fromRun``).  ``readRunMetadata`` checks the registration owner first,
and only then the cached PV-file / live-data lookup, so that the state is always derived from the data
actually being reduced, and a registered run never uses a cached result.

``ReservedRunNumber.LIVE`` is only for registered data: without a registration, its metadata is an error,
not a PV-file or listener lookup.  Its state can change between acquisitions, so the caches that are
keyed by run number as a stand-in for the state do not cache it by run number alone:
``LocalDataService.generateStateId`` does not cache it, and ``SousChef``'s pixel-group and peak caches
include its state in their keys.  (The grouping and instrument caches in ``GroceryService`` are keyed by
state already.)

Resolved questions
==================

* **Is chunk accumulation suspended while the post-processing script runs?**  This does not need to be
  handled by SNAPRed.  The registered workspace is cloned only on lookup, at the start of a fetch, as
  for the existing cached data; everything after that operates on the clone.  Any coordination
  between ``livereduce``'s accumulation and the post-processing script is a matter of the
  ``livereduce`` setup.
* **Which caches must be reset on a run transition?**  None are known.  Registered data bypasses the
  neutron-data and run-metadata caches, and the other caches manage their own lifetimes.  For
  ``ReservedRunNumber.LIVE``, whose state can change, see *Run metadata*.
* **Beam-monitor normalization** (``normalizeByBeamMonitor``): ``fetchMonitorWorkspace`` only loads the
  monitors from the NeXus file, so SNAPRed's own live-data mode does not support it either.  Registered
  data has the same limitation, and no monitor source is added for it.
* **Can an explicit live-data request be satisfied by a registered workspace?**  No: it is an error, as
  described under ``RegisteredSource``.

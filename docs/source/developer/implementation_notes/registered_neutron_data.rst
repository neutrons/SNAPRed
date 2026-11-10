Neutron data sources, and registered neutron data (live reduction under ``livereduce``)
-------------------------------------------------------------------------------------------

**Status: proposed design, not yet implemented.**

This design has three parts:

1. **Neutron data sources** (PR 1): a refactoring of the neutron-data fetch in ``GroceryService`` into
   a single system of *data sources*.  This changes no behavior, except that every fetched workspace is
   now tagged with its provenance.  The framework is designed for all of the kinds of data that
   ``GroceryService`` loads, not only neutron data.
2. **Registered neutron data** (PR 2): a new data source, for a workspace that is registered with
   SNAPRed by an external process, such as ``livereduce``.  With part 1 in place, this is one more
   source, plus its registration.
3. **Other data** (PR 3): diffraction-calibration tables and masks, normalization, grouping, pixel
   masks and monitor data are moved onto the same framework.

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
   of data and the resolution (native or lite) that it provides, and the resolution that it requires as
   input, if any.  The order in which sources are tried is configuration, not code.

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

The sources, the resolver and the registration owner go into a new package,
``snapred.backend.data.neutron``.  ``GroceryService`` keeps its public fetch methods
(``fetchNeutronDataSingleUse``, ``fetchNeutronDataCached``, ``clearLiveDataCache``, ...), and delegates to
the resolver.

Sources
~~~~~~~

.. code-block:: python

    class NeutronDataSource(ABC):
        kind: DataSourceKind                # nexusFile, liteFile, live, registered, liteConversion
        provides: Resolution                # native or lite
        requires: Resolution | None = None  # the resolution of the input data, for a transforming source
        cacheable: bool = True
        exclusive: bool = False

        def available(self, item: GroceryListItem) -> bool: ...
        def load(self, item: GroceryListItem, outputName: WorkspaceName,
                 input: SourcedData | None = None) -> SourcedData: ...

``SourcedData`` holds the workspace name, the provenance chain, and the loader that was used.

=========================  ===========  ==========  ==========  =========
source                     provides     requires    cacheable   exclusive
=========================  ===========  ==========  ==========  =========
``NexusFileSource``        native                   yes
``LiteFileSource``         lite                     yes
``LiveListenerSource``     native                   yes [#]_
``LiteConversionSource``   lite         native      yes
``RegisteredSource``       native                   **no**      **yes**
=========================  ===========  ==========  ==========  =========

.. [#] As now: live data is cached, and is removed by ``clearLiveDataCache``.

* ``NexusFileSource`` and ``LiteFileSource`` are not available when the item requests live data
  (``item.liveDataArgs`` is set), as now.
* ``LiveListenerSource`` keeps all of its existing behavior: the run-status checks, the run-number
  checks, the ``LiveDataState`` exceptions, and the different errors for an explicit live-data request
  and for the live-data fallback.
* ``LiteConversionSource`` clones its native input and converts the clone to lite mode.  It exports the
  converted data to disk **only when its input came from** ``nexusFile``.  This replaces the
  ``fromLiveData`` flag, and means that neither live nor registered data is ever exported: a lite-mode
  file of a partially-acquired run would otherwise be loaded later, by every file-based reduction of
  that run, as though it were the complete data.

Resolver
~~~~~~~~

The resolver produces the data for an item at the requested resolution:

1. If an *exclusive* source is available for the item, it is the only source that may start the chain.
   For a lite-mode request, its native data is then converted, by the source that provides lite mode
   from native.
2. Otherwise, the sources that provide the requested resolution are tried in the configured order.  The
   first available source is used.
3. If that source requires input, the input resolution is resolved first, in the same way, and passed to
   it.
4. If no source is available, the resolver raises an error that lists the sources that were tried.

The order is set in ``application.yml``:

.. code-block:: yaml

    neutronData:
      sources:
        native: [registered, nexusFile, live]
        lite: [liteFile, liteConversion]

Cache
~~~~~

The ``_loadedRuns`` cache becomes a layer *around* the resolver, not a step in each fetch.  It keeps
its current behavior (a raw workspace per run number and resolution, and numbered copies for each
cached fetch, including the cached native input of a lite-mode conversion), and it skips any result
whose provenance includes a source that is not cacheable.  A cache hit does not add to the provenance:
a copy of a cached workspace has the provenance of the original.

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

The same framework applies to the other kinds of data that ``GroceryService`` loads.  Each of these is
currently a separate method, with its own check for a resident workspace, its own load, and its own
cache.

=====================  ======================================  ===========================================
kind                   sources                                 cache
=====================  ======================================  ===========================================
diffCal table          indexed ``.h5`` file; generated         resident in the ADS, by name
                       default table (now
                       ``fetchDefaultDiffCalTable``)
diffCal mask           indexed ``.h5`` file                    resident in the ADS, by name
normalization          indexed file                            ``normalizationCache`` (a single entry)
grouping               grouping-definition file                ``_groupingCache``
reduction pixel mask   reduction-record file                   resident in the ADS, by name
monitor                NeXus file                              none
=====================  ======================================  ===========================================

* **Kind and key:** each source declares the kind of data it provides.  Each kind has its own key:
  run number and resolution for neutron and monitor data; state, version and resolution for
  calibration and normalization data.  The resolver is the same for every kind.
* **One output per source:** the diffCal table and mask sources share an implementation, which runs
  ``LoadCalibrationWorkspaces`` with only the one output requested (each of its outputs is optional).
  This matches how they are requested now: ``diffcal_table`` and ``diffcal_mask`` are already separate
  grocery items.  At present, either one loads both workspaces, so that applying a diffraction
  calibration in ``_processNeutronDataCopy`` also loads a mask that is not used.  Repeated fetches are
  cache hits, as now.
* **Native and lite share an implementation:** unlike neutron data, the native and lite versions of
  calibration and normalization data have no conversion between them.  Their sources use the same
  lookup inputs (run number, state and version) and the same loader; the resolution only selects which
  file the lookup finds.  So the native and lite sources of each of these kinds are one implementation,
  parameterized by resolution.
* **Cache:** the cache policy is set per kind, as above.
* **Processing steps:** the normalization load currently runs ``_processNeutronDataCopy`` on the
  loaded data.  This becomes an explicit processing step of that kind.
* **Provenance:** these workspaces are also tagged with ``dataSource``.  The existing ``diffcalState``
  and ``normalizationState`` tags are unchanged: they record what was *applied* to a sample workspace,
  not where a workspace came from.
* **Boundary:** a source decides how a product is brought into the ADS.  *Which* file and version
  contain it is still decided by the indexer and ``LocalDataService`` (including, on ``exp``, the
  read-only calibration failover), below the source layer.

Design: registered neutron data
===============================

Registration owner
~~~~~~~~~~~~~~~~~~

A dedicated ``@Singleton`` owns the run-number -> registered-workspace associations, and their
validation.  It is used by ``RegisteredSource`` and by the run-metadata lookup (below); it is exposed
through the registration endpoints.  No parameters are added to existing classes to support it.

* Registration should be performed from the *post-processing* script: only that script sees the
  accumulation workspace (its name is generated by ``livereduce`` as
  ``mtd.unique_name(prefix="accumulation_")``).  Re-registering on every update must be harmless.
* There is also an endpoint to remove a registration.
* **Validate the registration on each lookup:**

  - if the workspace no longer exists (for example, ``livereduce``'s memory watchdog has restarted the
    daemon and cleared the ADS), drop the registration: the source is then not available, and the
    resolver falls through to the other sources;
  - if its ``run_number`` log no longer matches, a run transition has occurred: drop the registration
    and raise ``LiveDataState.runStateTransition``.  The live listener does not always set
    ``run_number``, so a missing log value must be tolerated.

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

Resolved questions
==================

* **Is chunk accumulation suspended while the post-processing script runs?**  This does not need to be
  handled by SNAPRed.  The registered workspace is cloned only on lookup, at the start of a fetch, as
  for the existing cached data; everything after that operates on the clone.  Any coordination
  between ``livereduce``'s accumulation and the post-processing script is a matter of the
  ``livereduce`` setup.
* **Which caches must be reset on a run transition?**  None are known.  Registered data bypasses the
  neutron-data and run-metadata caches, and the other caches manage their own lifetimes.
* **Beam-monitor normalization** (``normalizeByBeamMonitor``): ``fetchMonitorWorkspace`` only loads the
  monitors from the NeXus file, so SNAPRed's own live-data mode does not support it either.  Registered
  data has the same limitation, and no monitor source is added for it.
* **Can an explicit live-data request be satisfied by a registered workspace?**  No: it is an error, as
  described under ``RegisteredSource``.

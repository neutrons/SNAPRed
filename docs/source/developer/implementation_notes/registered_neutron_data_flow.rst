Registered neutron data: the flow, and review questions
=======================================================

One ``livereduce`` update through SNAPRed, then answers to the review questions on #693 and #695.  The
design is in :doc:`data_sources`.

The flow of one update
----------------------

On each update, ``LoadLiveData`` adds the new chunk to the accumulation workspace, then runs the
post-processing script on it.  The script (``tests/cis_tests/livereduce/snapred_live_reduction.py``)
registers the accumulation workspace by name, then calls ``reduction/``.  The dotted arrows are every read
and write of the accumulation workspace.

.. mermaid::

    flowchart TD
        ACC[("accumulation workspace<br/>owned by livereduce")]

        subgraph mantid["Mantid: MonitorLiveData, every UpdateEvery seconds"]
            LLD["LoadLiveData: add the new chunk<br/>to the accumulation workspace"] --> PP["post-processing script<br/>input = the accumulation workspace"]
        end

        subgraph script["snapred_live_reduction.reduceAccumulation"]
            PP --> RN{"its 'run_number' log<br/>set, and not 0?"}
            RN -- yes --> NUM["run number = the log value"]
            RN -- "no: between runs,<br/>or before a run is set up" --> LIVE["run number = LIVE, 999999"]
        end

        subgraph snapred["SNAPRed, in the same process"]
            NUM --> REG["workspace/registerNeutronData<br/>NeutronDataRegistrations.register:<br/>the workspace must exist; for a real run number,<br/>a set 'run_number' log must match"]
            LIVE --> REG
            REG --> RED["reduction/"]
            RED --> STATE["state: generateStateId, then readRunMetadata,<br/>then RunMetadata.fromRun on the registered workspace's run,<br/>then DetectorState.fromPVLogs<br/>cached per run number, except for LIVE"]
            STATE --> RECS["calibration and normalization versions:<br/>the Indexer, for that state and run number"]
            RECS --> FETCH["input workspace: GroceryService.fetchNeutronDataSingleUse<br/>the reduction asks with dirty()"]
            FETCH --> CLONE["RegisteredSource, the only source for a registered run:<br/>CloneWorkspace to tof_all_RUN<br/>'run_number' set on the clone"]
            CLONE --> LITE{"lite mode?"}
            LITE -- yes --> CONV["LiteConversionSource: createLiteData<br/>tof_all_RUN to tof_all_lite_RUN, not exported"]
            CONV --> DEL["fetchSingleUse deletes tof_all_RUN"]
            DEL --> USE["ReductionRecipe, on its input workspace:<br/>unit conversion and preprocessing, in place"]
            LITE -- "no: tof_all_RUN is the input" --> USE
            USE --> OUT["reduced workspaces<br/>dataSource: registered, or registered>liteConversion"]
        end

        OUT --> CLEAN["the script deletes the outputs, the unfocused data,<br/>the combined pixel mask, and tof_all_lite_RUN;<br/>then checks that the accumulation workspace is unchanged"]

        LLD -. writes .-> ACC
        RN -. "reads the 'run_number' log" .-> ACC
        REG -. "reads the 'run_number' log" .-> ACC
        STATE -. "reads the logs" .-> ACC
        CLONE -. "reads the data" .-> ACC

``RUN`` is the zero-padded run number, for example ``tof_all_lite_073218``, or ``tof_all_lite_999999`` for
``LIVE``.

Who uses the registrations
--------------------------

``NeutronDataRegistrations`` maps a run number to a workspace name.

.. mermaid::

    flowchart LR
        WS["WorkspaceService<br/>registerNeutronData / unregisterNeutronData"] -- writes --> REG[("NeutronDataRegistrations<br/>@Singleton<br/>run number to workspace name")]
        RS["RegisteredSource<br/>in GroceryService.neutronData"] -- "isRegistered, lookup" --> REG
        GS["GroceryService.clearADS<br/>@Singleton"] -- "registeredWorkspaces: never deleted" --> REG
        LDS["LocalDataService.readRunMetadata<br/>@Singleton"] -- "lookup" --> REG
        GS -- "uses, as dataService" --> LDS

Review questions
----------------

Numbered as in the review comments on #695 (and #693).

Why does the test script remove the ``run_number`` log?  Where would the run number come from otherwise?  (#695, 0)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To test the case where the live-data listener leaves it unset.

The run number comes from the registration request.  The ``livereduce`` script uses the ``run_number`` log
if it is set and not 0, and ``LIVE`` otherwise (``RN``).  ``register`` checks a set log against a real run
number once (``REG``).  The registration's run number is written to the clone, not to the registered
workspace (``CLONE``).

The state is generated from the registered workspace's PV logs, as for a NeXus file (``STATE``).  The run
number only selects the calibration and normalization records for that state (``RECS``).

How do I register a workspace with no run number, or run number 0?  (#695, 5; #693, 4)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Register it under ``ReservedRunNumber.LIVE`` (``999999``).

* Its ``run_number`` log is not checked.
* ``LIVE`` is later than any real run, so the latest calibration and normalization for the state apply.
* Its state can change between updates, so ``generateStateId`` doesn't cache it, and ``SousChef``'s caches
  include the state in their keys.
* Without a registration, ``LIVE`` is an error.
* When the run starts, the script registers the workspace under the run number, which removes the ``LIVE``
  registration.
* As in SNAPRed's own live-data mode, nothing is written to disk: ``reduction/save`` saves nothing, and
  ``reduction/validate`` skips the write-permission check.

Tested with ``run_startlivedata.py --no-run-number``, across a change of state (73218, 73397, 73218).

How is the ``livereduce`` workspace protected from modification?  (#695, 4; #693, 3)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The script registers the accumulation workspace itself.  SNAPRed only reads it (the dotted arrows): its
logs at registration and for the state, and its data when it clones it for the reduction's input.  The
reduction modifies only that clone, or the lite workspace converted from it (``USE``).  ``clearADS`` never
deletes a registered workspace.

The accumulation workspace is not written while SNAPRed reads it: ``LoadLiveData`` runs the post-processing
script after adding the chunk.

The livereduce test checks, on every update, that the accumulation workspace is unchanged.

How does the lite-mode conversion avoid cloning, and caching, the native data?  (#695, 2; #693, 2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It doesn't avoid the clone.  It deletes it after the conversion, and caches nothing.

* ``RegisteredSource`` clones the accumulation workspace to ``tof_all_RUN``; ``createLiteData`` converts it
  to ``tof_all_lite_RUN``; ``fetchSingleUse`` then deletes ``tof_all_RUN`` (``CLONE``, ``CONV``, ``DEL``).
* Neither is cached: each update clones and converts the accumulation workspace again.
* Peak memory: the accumulation workspace, one native-size clone, and the lite workspace.

**Possible follow-up:** ``createLiteData`` could read the registered workspace directly, saving the native
clone.  This needs ``LiteDataRecipe`` to leave its input unchanged, and ``createLiteData`` to take the run
number as an argument instead of parsing it from the workspace name.

Is there an unnecessary ``@Singleton`` chain?  (#695, 1)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No.  The registrations aren't beneath ``GroceryService``.  ``WorkspaceService`` and ``LocalDataService`` use
them directly too (second figure).  Nesting them in any one of these would make the others reach them through
an unrelated singleton.

What does "process" mean in "an outside process registers"?  (#693, 1)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The registering code, not a separate OS process.  ``livereduce`` runs the post-processing script, and
SNAPRed, in its own Python process.  A registration is a workspace name in that process's ADS.

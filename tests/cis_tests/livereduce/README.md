# SNAPRed under livereduce: test setup and status

SNAPRed reduces the accumulation workspace that [livereduce](https://github.com/mantidproject/livereduce)
builds.  The post-processing script registers it as the neutron data for its run
(`workspace/registerNeutronData`), then reduces the run (`reduction/`); SNAPRed only sees the
registration.  The design is in `docs/source/developer/implementation_notes/data_sources.rst`.

## Files

| File | Role |
|---|---|
| `run_startlivedata.py` | Launcher without livereduce: calls Mantid's `StartLiveData` with the arguments livereduce builds, and the same post-processing script.  It stops after `--updates` updates (default `--chunks`), and exits with 0 only if every check passed.  Use this one for testing. |
| `run_livereduce.py` | Launcher: sets up Mantid's `FileEventDataListener` to replay a NeXus file, writes the livereduce config, runs livereduce's `scripts/livereduce.py` in the same process. |
| `reduce_ADARA_FileReader_live_post_proc.py` | The post-processing script livereduce runs on each update.  The name follows the replay instrument's short name; for the real SNAP listener the same body goes in `reduce_SNAP_live_post_proc.py`.  Its output is a single-valued workspace: `StartLiveData` requires one, and a copy of the accumulation workspace would double its memory. |
| `strip_run_number_proc.py` | Processing script for Tier 2: deletes the `run_number` log from each chunk.  `run_startlivedata.py --no-run-number` uses it.  livereduce finds its scripts by name, so it is not named `reduce_ADARA_FileReader_live_proc.py`: copy it to that name to use it under livereduce. |
| `inhouse_live_mode_check.py` | SNAPRed's own live-data mode (not livereduce), configured and driven as for the GUI, with the mock listener: prints the records used, the number of events reduced, a checksum of each reduced workspace, and the ADS. |
| `snapred_live_reduction.py` | `reduceAccumulation(input)`: registers (under `ReservedRunNumber.LIVE` if `run_number` is missing or 0), reduces, checks, and deletes what the reduction leaves in the ADS.  It writes `OK`/`FAIL` lines, the calibration and normalization versions, the memory (RSS before, peak, after, and the ADS contents), and `ALL OK` / `FAIL: n check(s) failed` per update to the `livereduce.snapred` logger: on stdout, and in `livereduce.log` under livereduce.  It records the result of each update in `RESULTS`.  Nothing is saved. |

## Running

livereduce is a service (signal handlers, restarts, file watching): what it adds to `StartLiveData` is not
what is being tested.  `run_startlivedata.py` runs the same post-processing script without it, and stops on
its own:

```
pixi run python tests/cis_tests/livereduce/run_startlivedata.py \
    --nexus /SNS/SNAP/IPTS-xxxxx/nexus/SNAP_xxxxx.nxs.h5 --chunks 3 [--no-run-number] [--no-reduction]
```

`--no-run-number` is Tier 2.  `--no-reduction` only logs each update and the memory: the baseline of the replay.
Several `--nexus` files are replayed in turn, in one process: with `--no-run-number`, that is a change of
the state of the live data (Tier 2b).  For each file, the calibration and normalization that must be used
are found from the state of the file, and checked; the pixel-grouping parameters of the reductions must be
the same within a state and different between states.

To run it under livereduce itself:

```
git clone https://github.com/mantidproject/livereduce <somewhere>
mkdir <workdir> && cd <workdir>
pixi run --manifest-path <SNAPRed> python <SNAPRed>/tests/cis_tests/livereduce/run_livereduce.py \
    --livereduce <somewhere>/livereduce/scripts/livereduce.py \
    --nexus /SNS/SNAP/IPTS-xxxxx/nexus/SNAP_xxxxx.nxs.h5 \
    --chunks 3 --update-every 30
grep -E "OK  |FAIL|ALL OK|update: run" livereduce.log
```

* `--nexus` must be a raw event file (`/entry`), not processed NeXus.
* `--ipts-root` points SNAPRed at another data tree.  Without it, SNAPRed's configured `IPTS.root` (`/SNS`)
  is used: that is the analysis-cluster case.
* The reduction home is a temporary directory.
* When the whole file has been read, `MonitorLiveData` logs "The whole file has been read!".  livereduce then
  restarts the listener, so stop it with Ctrl-C.
* livereduce only needs Mantid and `psutil`, which the SNAPRed pixi env has.

## Test tiers

| Tier | What | Status |
|---|---|---|
| 0 | `tests/cis_tests/registered_neutron_data_script.py`: registration without livereduce | done |
| 1 | livereduce replays a run that has a `run_number`; post_proc registers under it and reduces in lite mode | **done** on the analysis cluster, with `run_startlivedata.py` |
| 2 | Same, but the `run_number` log is removed from every chunk (live data between runs, or before a run is set up); post_proc registers under `ReservedRunNumber.LIVE` | **done** on the analysis cluster, with `run_startlivedata.py --no-run-number` |
| 2b | Tier 2 across state changes: 73218 → 73397 → 73218 in one process, without run numbers | **done** on the analysis cluster (below) |
| 3 | Real SNAP listener on `bl3-daq1`, script in `/SNS/SNAP/shared/livereduce` | later, with the beamline team |

## Status (2026-11-19)

**Tiers 1, 2 and 2b pass on `analysis-node01`** with `run_startlivedata.py`, replaying
`/SNS/SNAP/IPTS-37478/nexus/SNAP_72380.nxs.h5` (`--chunks 3`, no `--ipts-root`).  Its state, `0f78eb70a1c029b7`,
has lite calibration v3 and normalization v2 for `>=72368`.

* 3 updates, 68M, 129M, then 217M events; `ALL OK` on each, and exit status 0.  The reduction uses
  calibration v3 and normalization v2, under run 72380 (Tier 1) and under `LIVE`, `999999` (Tier 2).
* Each update (registration, then lite reduction to the `all`, `bank`, `column`, and `5deg` groupings)
  takes 25-31 s, so `update_every` should be at least about 30 s for this run size.

### Tier 2: `ReservedRunNumber.LIVE`

Live data without a run number is registered under `ReservedRunNumber.LIVE` (`999999`).  It is numeric and
later than any real run, so the `Indexer` needed no change: the latest open-ended calibration and
normalization for the state apply (the instrument-parameter lookup goes through the same `Indexer`).  The
state comes from the registered workspace's logs.  What did change:

* `NeutronDataRegistrations`: `lookup` no longer checks the `run_number` log; `register` checks it only
  for a real run number.  `RegisteredSource` gives its clone the run number of the registration.
* `ReservedRunNumber.unmodifiedInstrument()` (`NATIVE`, `LITE`) replaces `ReservedRunNumber.values()` where
  the reserved run numbers meant "the unmodified instrument, with a reserved state".
* The state of `LIVE` can change between acquisitions: `generateStateId` does not cache it, and SousChef's
  pixel-group and peak caches include its state in their keys.  Without a registration, `LIVE` is an
  error, not a PV-file or listener lookup.

Saving a `LIVE` reduction is not handled: it has no IPTS.

### Tier 2b: the state of `LIVE` changes

```
run_startlivedata.py --no-run-number --chunks 2 \
    --nexus /SNS/SNAP/IPTS-37636/nexus/SNAP_73218.nxs.h5 /SNS/SNAP/IPTS-37478/nexus/SNAP_73397.nxs.h5 \
            /SNS/SNAP/IPTS-37636/nexus/SNAP_73218.nxs.h5
```

| File | State | Calibration (run, version) | Normalization | Pixel grouping |
|---|---|---|---|---|
| 73218 | `70237edd552f39a4` | 73218, v1 | 73400, v0 | `29e63a3eada1` |
| 73397 | `0721dd3000a59c89` | 73397, v1 | 73398, v0 | `3da45821261d` |
| 73218 | `70237edd552f39a4` | 73218, v1 | 73400, v0 | `29e63a3eada1` |

All 6 updates `ALL OK`, all registered as `999999`.  Negative controls (with the source changes undone,
then restored):

* without the `generateStateId` and SousChef changes, the reduction of the second state fails:
  `XMin is greater than the largest X value` (the first state's cached state is used);
* without the SousChef change only, **the reduction of the second state succeeds, silently wrong**: the
  right calibration and normalization, but the pixel-grouping parameters of the first state.  Only the
  pixel-grouping check catches it.

### SNAPRed's own live-data mode (not livereduce)

The PR moved the in-house live-data fetch onto `LiveListenerSource`.  `inhouse_live_mode_check.py <nexus>
[--chunks N]` checks it end to end with the mock listener, configured as for the GUI
(`liveData.facility.name: TEST_LIVE`, `liveData.instrument.name: ADARA_FileReader`, and
`liveData.testInput`), making the GUI's requests: `reduction/hasLiveDataConnection`,
`reduction/getLiveMetadata`, then `reduction/` with `liveDataMode`, twice.

On SNAP_73218: both reductions succeed with calibration 73218 v1 and normalization 73400 v0, and reduce all
1,608,362 events, with 3 chunks as with 1.  The six reduced workspaces are identical (sums of X, Y and Y²)
to those of `origin/next` (which could only be run with the workarounds below, and 1 chunk).  The ADS
differs only as intended: this branch doesn't leave the native `tof_all_<run>` in the ADS (2.6M events),
and the outputs carry `dataSource` tags (`live>liteConversion`).

The mock listener could not drive SNAPRed's live mode, on `origin/next` either, so the GUI couldn't use it.
Fixed in the live-data flow, when `snapred.meta.mantid.liveDataReplay.usingFileReplay()`:

* `hasLiveDataConnection`: the mock listener's address has port 0, which `_parseSocketAddress` rejects
  ("Port out of range: 0").  It is now available when its file (`fileeventdatalistener.filename`) is found.
* `RunStatus.from_run`: a replayed chunk has the `end_time` (and final run-state) of the recorded run, so
  it was `STOPPED`, and `LoadLiveDataInterval` raised "cannot extract initial chunk from inactive run".
  These are now ignored for the replay; abort and pause still apply.
* `LoadLiveDataInterval`: the mock listener's chunks divide the events of the file, not its time, so the
  time-coverage test passed after the first chunk, and **a live reduction got 1/N of the data**.  After
  extracting a chunk the listener starts loading the next one in the background, and its destructor waits
  for that load, which waits for the GIL to log (Mantid's log goes to Python): so stopping early also
  **deadlocked**, and an exception in `LoadLiveDataInterval` ended the same way, unreported.  It now reads
  every chunk (without the waits for new data), and after an error reads the remaining chunks before the
  listener is released.

`readLiveMetadata` still leaves its listener's next chunk loading; that is harmless (the algorithm is kept
by the `AlgorithmManager` until long after the load has finished), but holds one chunk in memory.

### Memory

Peak RSS 42-45 GB at 217M events, down from 52 GB.  Measured per update (the `memory:` lines):

| | RSS after update 1 / 2 / 3 |
|---|---|
| replay only (`--no-reduction`) | 3.0 / 7.7 / 12.6 GB |
| with the reduction, before | 33.4 / 41.0 / 46.5 GB (ADS 7, 9, 10 workspaces) |
| with the reduction, now | 33.4 / 39.4 / 40.0 GB (ADS 5, 6, 6 workspaces) |

* **The first reduction in a process costs about 27-30 GB that stays resident**, whatever the data size
  (16M or 217M events), and it is the same for an ordinary reduction from the NeXus file, on
  `origin/next` too.  It is not in the ADS, and `malloc_trim` doesn't release it.  Reducing the same
  registered workspace 5 times: 32.7, 32.9, 32.9, 32.9, 33.0 GB, so there is no leak per reduction.
* The post_proc no longer clones the accumulation workspace as its output (a full-size copy), and deletes
  the two workspaces each reduction leaves in the ADS: the combined pixel mask
  `pixelmask_<run>_<timestamp>` (one per update otherwise), and the reduction's input workspace
  `tof_all_lite_<run>` (as large as the accumulation, in lite mode).
* **Bug, also on `origin/next`:** reducing the same run twice from its file, in one process, fails the
  second time ("The workspace must have units of TOF").  The reduction crops and converts its input
  workspace `tof_all_lite_<run>` in place and leaves it in the ADS, and `GroceryService.fetchWorkspace`
  reuses an existing workspace of that name instead of loading the file.  Registered data is not affected
  (the lite conversion overwrites it).

Earlier, verified locally by replaying `tests/data/snapred-data/SNS/SNAP/IPTS-24641/nexus/SNAP_46680.nxs.h5`
(`--chunks 3 --update-every 10 --ipts-root tests/data/snapred-data/SNS`):

* The replay works with the settings livereduce forces (`FromStartOfRun`, `Add`, `PreserveEvents`).  There
  were 3 updates, and the accumulation workspace grew from 1.7M to 4.0M to 6.7M events, with `run_number`
  46680 and the SNAP instrument.
* On each update, registration succeeds, and the accumulation workspace is unchanged: it still exists, has
  no `dataSource` tag, and has the same event count.
* **The reduction fails, because of the test data, not the code.**  In `snapred-data`, `SNAP_46680` is the
  only real SNAP event file, and its state (`04bd2c53f6bf6754`) has records in an old format:
  * calibration v1 applies only to `>=58882`, so 46680 gets the default v0 record;
  * that record has no `workspaces` field;
  * its table is named `_diffract_consts_default_v0.h5`, with the old leading underscore;
  * the normalization v0 record has `calibrationVersionUsed: "0xF"`.

  The 588xx files are ultralite CRACKLE processed NeXus, which the replay listener can't read.

Points that came up while getting here:

* The `reduction/` payload must be the `ReductionRequest` object, as the GUI sends it.  A JSON string
  breaks the `WallClockTime` decorator, which runs before `FromString`.
* The post_proc `input` is a workspace object, not a name.
* The `ReductionRequest` needs a `timestamp` from `reduction/getUniqueTimestamp`, as the GUI gets one.  Without
  it, `ReductionIngredients` fails validation.
* `StartLiveData` starts `MonitorLiveData` asynchronously: a script waiting for updates must not take
  "not running yet" for "stopped", nor cancel it before it has started, or it leaves the monitor running:
  the process never exits, and the next `StartLiveData` with the same output workspace is refused.
* On `analysis`, Claude Code's auto mode blocked the livereduce launcher (a freshly cloned service, run in
  the background) but not `run_startlivedata.py`.
* SousChef caches grow by a pixel-group and a peak entry per grouping, per reduction: their keys include the
  timestamped pixel mask.  They are small, but grow without bound in a long live session.
* The SNAPRed test env (`env=*test`) only works under pytest (`"conftest" in sys.modules`).  Standalone,
  use `IPTS_override` / `Config_override`, as `run_livereduce.py` does.

## Next steps

1. **Commit** the Tier 1 / Tier 2 / memory work (uncommitted as of 2026-11-19).
2. **The ~27 GB first-reduction cost** (not specific to live reduction): find what holds it.
3. **The reduction leaves its input workspace in the ADS** (the bug above): fix it in SNAPRed, rather
   than in the post_proc.  It changes what the GUI finds in the ADS after a reduction.
4. **Optional: refresh the 46680 state in `snapred-data`** with current SNAPRed, so that a full local
   reduction works.  It's a separate change; use `pixi run git` because of LFS.
5. **Open review question (ekapadi, #693 / #695):** the `@Singleton` chain under `GroceryService`.
6. Tier 3, with the beamline team.

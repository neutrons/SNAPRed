# SNAPRed under livereduce: test setup and status

SNAPRed reduces the accumulation workspace that [livereduce](https://github.com/mantidproject/livereduce)
builds.  The post-processing script registers it as the neutron data for its run
(`workspace/registerNeutronData`), then reduces the run (`reduction/`); SNAPRed only sees the
registration.  The design is in `docs/source/developer/implementation_notes/data_sources.rst`.

## Files

| File | Role |
|---|---|
| `run_livereduce.py` | Launcher: sets up Mantid's `FileEventDataListener` to replay a NeXus file, writes the livereduce config, runs livereduce's `scripts/livereduce.py` in the same process. |
| `reduce_ADARA_FileReader_live_post_proc.py` | The post-processing script livereduce runs on each update.  The name follows the replay instrument's short name; for the real SNAP listener the same body goes in `reduce_SNAP_live_post_proc.py`. |
| `snapred_live_reduction.py` | `reduceAccumulation(input)`: registers, reduces, and checks.  It writes `OK`/`FAIL` lines and `ALL OK` / `FAIL: n check(s) failed` per update to the Mantid log, which livereduce forwards to `livereduce.log`.  Nothing is saved. |

## Running

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
| 1 | livereduce replays a run that has a `run_number`; post_proc registers under it and reduces in lite mode | plumbing verified locally; **full reduction still to do, on the analysis cluster** |
| 2 | Same, but the `run_number` log is removed from every chunk (live data between runs, or before a run is set up) | needs the `LIVE` stand-in (below) |
| 3 | Real SNAP listener on `bl3-daq1`, script in `/SNS/SNAP/shared/livereduce` | later, with the beamline team |

## Status (2026-11-19)

Verified locally by replaying `tests/data/snapred-data/SNS/SNAP/IPTS-24641/nexus/SNAP_46680.nxs.h5`
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
* The SNAPRed test env (`env=*test`) only works under pytest (`"conftest" in sys.modules`).  Standalone,
  use `IPTS_override` / `Config_override`, as `run_livereduce.py` does.

## Next steps

1. **Tier 1 on the analysis cluster.**  Choose a recent SNAP run with current calibration and
   normalization for its state, and run the command above with `--nexus` set to its `/SNS` file and no
   `--ipts-root`.  Expect `ALL OK` on every update.  Then check:
   * the reduction time against `--update-every`;
   * memory: the only full-size copy per update should be the temporary clone of the accumulation
     workspace, deleted after the lite conversion.
2. **Tier 2: live data without a run number** (ekapadi, review of #693 / #695):
   * add `ReservedRunNumber.LIVE`.  The post_proc registers under it when `run_number` is missing or 0.
     `snapred_live_reduction.py` currently fails that case on purpose.
   * `Indexer` treats `LIVE` as newer than every run, so that calibration and normalization are the
     latest for the state, with the state taken from the workspace logs.  The instrument-parameter
     lookup gets the same rule.
   * drop the `run_number` check in `NeutronDataRegistrations.lookup`.  In the registered case,
     livereduce handles run changes and accumulation; SNAPRed doesn't.  Keep the one-time check in
     `register` for a real run number.
   * test it with a `reduce_ADARA_FileReader_live_proc.py` that deletes the `run_number` log from each
     chunk.
3. **Optional: refresh the 46680 state in `snapred-data`** with current SNAPRed, so that a full local
   reduction works.  It's a separate change; use `pixi run git` because of LFS.
4. **Open review question (ekapadi, #693 / #695):** the `@Singleton` chain under `GroceryService`.

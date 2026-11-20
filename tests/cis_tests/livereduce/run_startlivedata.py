"""
Run the livereduce post-processing script under Mantid's ``StartLiveData``, without livereduce itself.

    pixi run python tests/cis_tests/livereduce/run_startlivedata.py --nexus <SNAP_xxxxx.nxs.h5> [options]

livereduce (https://github.com/mantidproject/livereduce) is a service: it builds the ``StartLiveData``
arguments from its configuration, and adds signal handling, restarts, and file watching.  This script
calls ``StartLiveData`` with the same arguments (``FromStartOfRun``, ``AccumulationMethod='Add'``,
``PreserveEvents``, an accumulation workspace, and the post-processing script in this directory), so the
post-processing script runs as it does under livereduce.  It then waits for `--updates` updates, or for
the end of the file, and cancels ``MonitorLiveData``.

* The NeXus file is replayed by Mantid's `FileEventDataListener` (facility 'TEST_LIVE', instrument
  'ADARA_FileReader'), in `--chunks` chunks.  It must be a raw event file ('/entry'), not processed NeXus.
* `--ipts-root` points SNAPRed at another data tree (e.g. 'tests/data/snapred-data/SNS'); by default
  the `IPTS.root` from SNAPRed's configuration ('/SNS') is used.  The reduction home is always a
  temporary directory: nothing is saved.
* `--no-run-number` deletes the 'run_number' log from each chunk (with 'strip_run_number_proc.py'), as for
  live data between runs: the data is then registered under `ReservedRunNumber.LIVE`.
* With several `--nexus` files, each is replayed in turn in the same process, as livereduce restarts live
  data on a run transition: ``MonitorLiveData`` is cancelled, and the next file is accumulated in a new
  accumulation workspace.  With `--no-run-number`, this is a change of the state of the live data.
* For each file, the calibration and normalization that the reduction must use are found from the state of
  the file (from its own run number).  The pixel-grouping parameters of the reductions must be the same
  for files in the same state, and different for files in different states.
* The exit status is 0 only if there were `--updates` updates for each file, and every check passed.
"""

import argparse
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
POST_PROCESSING_SCRIPT = HERE / "reduce_ADARA_FileReader_live_post_proc.py"
STRIP_RUN_NUMBER_SCRIPT = HERE / "strip_run_number_proc.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--nexus", required=True, type=Path, nargs="+", help="raw event NeXus file(s) to replay, in turn"
    )
    parser.add_argument("--chunks", type=int, default=3, help="number of chunks the file is replayed in")
    parser.add_argument("--updates", type=int, default=None, help="stop after this many updates (default: --chunks)")
    parser.add_argument("--update-every", type=float, default=1.0, help="'UpdateEvery' (seconds)")
    parser.add_argument("--timeout", type=float, default=1800.0, help="give up after this many seconds")
    parser.add_argument("--ipts-root", type=Path, default=None, help="SNAPRed 'IPTS.root' override")
    parser.add_argument(
        "--no-run-number",
        action="store_true",
        help="delete the 'run_number' log from each chunk: the data is registered under `ReservedRunNumber.LIVE`",
    )
    parser.add_argument(
        "--no-reduction", action="store_true", help="don't register or reduce: the memory baseline of the replay"
    )
    args = parser.parse_args()
    updates = args.updates if args.updates is not None else args.chunks

    sys.path[:0] = [str(HERE), str(REPO / "tests")]

    import snapred_live_reduction  # the module the post-processing script imports

    snapred_live_reduction.REDUCE = not args.no_reduction
    from mantid.kernel import ConfigService
    from util.Config_helpers import Config_override
    from util.IPTS_override import IPTS_override

    # As livereduce does: the replay instrument belongs to the 'TEST_LIVE' facility.
    ConfigService.setFacility("TEST_LIVE")
    ConfigService.setString("fileeventdatalistener.chunks", str(args.chunks))

    segments = []  # per file: (run number, state, number of updates, pixel-grouping fingerprints)
    with ExitStack() as stack:
        if args.ipts_root is not None:
            stack.enter_context(IPTS_override(str(args.ipts_root.resolve())))
        reductionHome = stack.enter_context(tempfile.TemporaryDirectory(prefix="snapred_livereduce_"))
        stack.enter_context(Config_override("instrument.reduction.home", reductionHome))

        for index, nexus in enumerate(args.nexus):
            runNumber, state, expected = _expected(nexus, args.no_run_number)
            print(f"=== file {index + 1} of {len(args.nexus)}: {nexus.name}, run {runNumber}, state {state}")
            snapred_live_reduction.EXPECTED = expected
            nResults, nGrouping = len(snapred_live_reduction.RESULTS), len(snapred_live_reduction.PIXEL_GROUPING)
            _replay(nexus, f"accumulation_{index + 1}", args, nResults + updates)
            segments.append(
                (
                    runNumber,
                    state,
                    len(snapred_live_reduction.RESULTS) - nResults,
                    snapred_live_reduction.PIXEL_GROUPING[nGrouping:],
                )
            )

    results = snapred_live_reduction.RESULTS
    ok = all(results) and all(n >= updates for _, _, n, _ in segments)
    print(f"{len(results)} update(s), {updates} expected per file; passed: {results}")
    if not args.no_reduction:
        ok = _checkPixelGrouping(segments) and ok
    print("ALL UPDATES OK" if ok else "FAILED")
    return 0 if ok else 1


def _expected(nexus: Path, noRunNumber: bool):
    # The state of the file, from its own run number, and the records that apply to the data as it is
    #   registered: under `ReservedRunNumber.LIVE` without a run number, otherwise under the run number.
    import h5py

    from snapred.backend.data.LocalDataService import LocalDataService
    from snapred.meta.InternalConstants import ReservedRunNumber

    with h5py.File(nexus, "r") as h5:
        runNumber = h5["/entry/run_number"][0].decode()
    service = LocalDataService()
    state, _ = service.generateStateId(runNumber)
    registeredAs = ReservedRunNumber.LIVE.value if noRunNumber else runNumber
    expected = {}
    for name, indexer in (
        ("calibration", service.calibrationIndexer(True, state)),
        ("normalization", service.normalizationIndexer(True, state)),
    ):
        entry = indexer.latestApplicableEntry(registeredAs)
        expected[name] = (str(entry.runNumber), int(entry.version)) if entry is not None else None
    print(f"expected for run {runNumber} registered as {registeredAs}: {expected}")
    return runNumber, state, expected


def _replay(nexus: Path, accumulation: str, args, untilResults: int):
    import snapred_live_reduction
    from mantid.api import AlgorithmManager
    from mantid.kernel import ConfigService
    from mantid.simpleapi import DeleteWorkspace, mtd

    ConfigService.setString("fileeventdatalistener.filename", str(nexus.resolve()))

    # As livereduce builds them (`Config.toStartLiveArgs`), with a post-processing script.
    start = AlgorithmManager.create("StartLiveData")
    start.initialize()
    if args.no_run_number:
        start.setProperty("ProcessingScriptFilename", str(STRIP_RUN_NUMBER_SCRIPT))
    for name, value in dict(
        Instrument="ADARA_FileReader",
        FromNow=False,
        FromStartOfRun=True,
        UpdateEvery=args.update_every,
        PreserveEvents=True,
        AccumulationMethod="Add",
        PostProcessingScriptFilename=str(POST_PROCESSING_SCRIPT),
        AccumulationWorkspace=accumulation,
        OutputWorkspace="result",
    ).items():
        start.setProperty(name, value)
    start.execute()  # the first update runs here
    monitor = start.getProperty("MonitorLiveData").value

    # `MonitorLiveData` is started asynchronously: it may not be running yet.
    deadline = time.monotonic() + args.timeout
    started = False
    try:
        while len(snapred_live_reduction.RESULTS) < untilResults:
            running = monitor is not None and monitor.isRunning()
            started = started or running
            if monitor is None or (not running and (started or monitor.isExecuted())):
                print("MonitorLiveData has stopped")
                break
            if time.monotonic() > deadline:
                print(f"timed out after {args.timeout} s")
                break
            time.sleep(0.5)
    finally:
        if monitor is not None:
            # It may still be about to start (for example, when the first update was the last one wanted):
            #   cancelling it before it runs would leave it running.
            startDeadline = time.monotonic() + 30.0
            while not (monitor.isRunning() or monitor.isExecuted()) and time.monotonic() < startDeadline:
                time.sleep(0.1)
            if monitor.isRunning():
                monitor.cancel()
                while monitor.isRunning():
                    time.sleep(0.1)
        # As livereduce does when it restarts live data: the workspaces are cleared.
        for workspace in (accumulation, "result"):
            if mtd.doesExist(workspace):
                DeleteWorkspace(workspace)


def _checkPixelGrouping(segments) -> bool:
    # Within a file, every reduction has the same pixel-grouping parameters.  Between files, they are the same
    #   if and only if the states are.
    ok = True
    fingerprints = {}
    for runNumber, state, _, prints in segments:
        if len(set(prints)) != 1:
            print(f"FAIL run {runNumber}: pixel grouping varies, or is missing, within the file: {prints}")
            ok = False
            continue
        fingerprints.setdefault(state, set()).add(prints[0])
    for state, prints in fingerprints.items():
        if len(prints) != 1:
            print(f"FAIL state {state}: different pixel grouping for the same state: {sorted(prints)}")
            ok = False
    byPrint = {}
    for state, prints in fingerprints.items():
        for p in prints:
            byPrint.setdefault(p, set()).add(state)
    for p, states in byPrint.items():
        if len(states) != 1:
            print(f"FAIL pixel grouping {p} is used for different states: {sorted(states)}")
            ok = False
    if ok:
        print(f"OK   pixel grouping follows the state: { {s: next(iter(p)) for s, p in fingerprints.items()} }")
    return ok


if __name__ == "__main__":
    sys.exit(main())

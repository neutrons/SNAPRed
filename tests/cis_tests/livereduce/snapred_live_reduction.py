"""
Reduce a ``livereduce`` accumulation workspace with SNAPRed, as the post-processing script does it.

The accumulation workspace is registered as the neutron data for its run, through the
``workspace/registerNeutronData`` endpoint, and the run is then reduced through the ``reduction``
endpoint.  SNAPRed does not know where the data came from: it only finds the registration.

Live data without a run number (its 'run_number' log is missing, or 0: between runs, or before a run has
been set up) is registered under `ReservedRunNumber.LIVE`.

The checks are logged (to stdout, and by livereduce to 'livereduce.log') as ``OK``/``FAIL`` lines, then one
summary line per update: ``ALL OK`` or ``FAIL: <n> check(s) failed``.  Nothing is saved.

Each update also logs the process's memory (RSS) before and after, and its peak during the update.
"""

import ctypes
import hashlib
import json
import threading
from contextlib import contextmanager
from typing import Callable, Dict, List, Optional, Tuple

import psutil
from mantid.simpleapi import DeleteWorkspace, mtd

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.request import RegisterNeutronDataRequest
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.InternalConstants import ReservedRunNumber
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

# One entry per call of `reduceAccumulation`: whether every check passed.  `run_startlivedata.py` reads it,
#   since the post-processing script runs in the same interpreter.
RESULTS: List[bool] = []

# The (run number, version) of the calibration and of the normalization that the reduction must use, if set.
#   `run_startlivedata.py` sets it for each file, from the state of the file.
EXPECTED: Optional[Dict[str, Tuple[str, int]]] = None

# One entry per successful reduction: a fingerprint of its pixel-grouping parameters.  These are computed
#   from the detector geometry of the state, and cached by `SousChef`: they show which state was used.
PIXEL_GROUPING: List[str] = []

# When `False`, nothing is registered or reduced: only the update and the memory are logged.  This is the
#   baseline for the memory of the replay itself.
REDUCE = True

# The SNAPRed handler prints it; and it propagates to the root logger, where livereduce writes 'livereduce.log'.
logger = snapredLogger.getLogger("livereduce.snapred")


def _request(path: str, payload):
    # As from the GUI: the payload is the request object.
    return InterfaceController().executeRequest(SNAPRequest(path=path, payload=payload))


def _log(workspace, name: str):
    run = mtd[str(workspace)].getRun()
    return run.getProperty(name).value if run.hasProperty(name) else None


@contextmanager
def _memory(report: Callable[[str], None], interval: float = 0.1):
    # Log RSS before and after, and sample its peak in between.
    process = psutil.Process()
    GB = 1024.0**3
    before = process.memory_info().rss
    peak = [before]
    done = threading.Event()

    def sample():
        while not done.wait(interval):
            peak[0] = max(peak[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        yield
    finally:
        done.set()
        sampler.join()
        after = process.memory_info().rss
        peak[0] = max(peak[0], after)
        # What the allocator keeps, but doesn't use: freed memory that glibc hasn't returned to the system.
        ctypes.CDLL("libc.so.6").malloc_trim(0)
        trimmed = process.memory_info().rss
        report(
            f"memory: RSS {before / GB:.1f} GB before, peak {peak[0] / GB:.1f} GB, {after / GB:.1f} GB after"
            f" ({trimmed / GB:.1f} GB after `malloc_trim`);"
            f" {len(mtd.getObjectNames())} workspaces in the ADS"
        )
        for name in sorted(mtd.getObjectNames()):
            workspace = mtd[name]
            size = workspace.getMemorySize() if hasattr(workspace, "getMemorySize") else 0
            report(f"    {size / GB:8.3f} GB  {workspace.id():22s} {name}")


def _fingerprint(pixelGroupingParameters) -> str:
    dump = {name: [p.model_dump(mode="json") for p in params] for name, params in pixelGroupingParameters.items()}
    return hashlib.sha1(json.dumps(dump, sort_keys=True).encode()).hexdigest()[:12]


class Checks:
    def __init__(self, report: Callable[[str], None]):
        self.report = report
        self.failures: List[str] = []

    def __call__(self, description: str, condition: bool):
        self.report(f"{'OK  ' if condition else 'FAIL'} {description}")
        if not condition:
            self.failures.append(description)

    def summary(self) -> str:
        return f"FAIL: {len(self.failures)} check(s) failed" if self.failures else "ALL OK"


def reduceAccumulation(accumulation, useLiteMode: bool = True, report: Callable[[str], None] = logger.info) -> bool:
    """
    Register the accumulation workspace for its run, and reduce the run.

    `accumulation` is the workspace (or its name) that ``StartLiveData`` passes to the post-processing
    script.  Returns whether every check passed.
    """
    with _memory(report):
        passed = _reduceAccumulation(str(accumulation), useLiteMode, report)
    RESULTS.append(passed)
    return passed


def _reduceAccumulation(accumulation: str, useLiteMode: bool, report: Callable[[str], None]) -> bool:
    check = Checks(report)
    dataSourceLog = f"{Config['metadata.tagPrefix']}dataSource"

    logRunNumber = _log(accumulation, "run_number")
    runNumber = str(logRunNumber) if logRunNumber and int(logRunNumber) != 0 else ReservedRunNumber.LIVE.value
    eventCount = mtd[accumulation].getNumberEvents()
    report(f"update: run {runNumber} ('run_number' log: {logRunNumber}), {eventCount} events in '{accumulation}'")
    if not REDUCE:
        return True

    response = _request(
        "workspace/registerNeutronData", RegisterNeutronDataRequest(runNumber=runNumber, workspace=accumulation)
    )
    check("registration succeeds", response.code == ResponseCode.OK)

    # As the GUI does, before each reduction.
    timestamp = InterfaceController().executeRequest(SNAPRequest(path="reduction/getUniqueTimestamp")).data
    response = _request(
        "reduction/", ReductionRequest(runNumber=runNumber, useLiteMode=useLiteMode, timestamp=timestamp)
    )
    check(f"reduction succeeds ({response.code}: {response.message or ''})", response.code == ResponseCode.OK)

    # The reduction leaves its combined pixel mask in the ADS (there would be one per update), and its input
    #   workspace (as large as the accumulation workspace, in lite mode).  The input workspace has been cropped
    #   and converted in place: `GroceryService.fetchWorkspace` would reuse it, for a run reduced from its file.
    leftovers = [
        wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build(),
        wng.run().runNumber(runNumber).lite(wng.Lite.fromLiteMode(useLiteMode)).build(),
    ]
    if response.code == ResponseCode.OK:
        record = response.data.record
        used = {
            name: (str(r.runNumber), int(r.version)) if r is not None else None
            for name, r in (("calibration", record.calibration), ("normalization", record.normalization))
        }
        fingerprint = _fingerprint(record.pixelGroupingParameters)
        PIXEL_GROUPING.append(fingerprint)
        report(
            f"calibration {used['calibration']}, normalization {used['normalization']} (run number, version);"
            f" pixel grouping {fingerprint}"
        )
        if EXPECTED is not None:
            for name, expectedRecord in EXPECTED.items():
                check(f"the {name} is {expectedRecord} (it is {used[name]})", used[name] == expectedRecord)
        outputs = record.workspaceNames
        check("reduction has output workspaces", len(outputs) > 0)
        expected = "registered>liteConversion" if useLiteMode else "registered"
        for workspace in outputs:
            if mtd.doesExist(workspace) and hasattr(mtd[workspace], "getRun"):
                check(f"'{workspace}' comes from the registered data", _log(workspace, dataSourceLog) == expected)
        leftovers += [*outputs, response.data.unfocusedData]
    for workspace in leftovers:
        if workspace and mtd.doesExist(workspace):
            DeleteWorkspace(workspace)

    check("the accumulation workspace still exists", mtd.doesExist(accumulation))
    check("the accumulation workspace has no dataSource tag", _log(accumulation, dataSourceLog) is None)
    check("the accumulation workspace has the same events", mtd[accumulation].getNumberEvents() == eventCount)
    report(check.summary())
    return not check.failures

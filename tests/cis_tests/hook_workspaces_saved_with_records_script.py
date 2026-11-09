"""
CIS script: the workspaces named by hook arguments are saved with each persisted record.

Writing a calibration, normalization or reduction record also writes every workspace that
one of the record's hook arguments names as a `WorkspaceName`, into the record directory's
`hook.workspaces.directory` subdirectory. The save method follows the workspace type:
mask and grouping workspaces in 'SaveDiffCal' format, anything else in nexus format.

This checks, for each of the three record types, that:
  * exactly the `WorkspaceName` arguments are saved -- a plain-string argument is not;
  * a workspace named by several arguments is saved once;
  * a workspace that no longer exists is skipped, with a warning;
and that the saved files reload as the right workspace types, and that a record read back
from disk still knows which of its hook arguments are workspaces.

SAFETY: this uses the test configuration and the fake SNAP instrument, and points the
calibration and reduction roots at a scratch directory. Nothing outside it is written.

Run from the repository root with
    pixi run python tests/cis_tests/hook_workspaces_saved_with_records_script.py [outputDir]
The output is left in `outputDir` (default: a new temporary directory) for inspection.
"""

import logging
import os
import sys
import tempfile
import types
from pathlib import Path

# Use the test resources (tests/resources/application.yml): SNAPRed selects them only when
#   the environment is a test environment and a "conftest" module is loaded.
os.environ.setdefault("env", "test")
sys.modules.setdefault("conftest", types.ModuleType("conftest"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mantid.simpleapi import (  # noqa: E402
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadDiffCal,
    LoadEmptyInstrument,
    LoadNexus,
    mtd,
)
from util.dao import DAOFactory  # noqa: E402
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask  # noqa: E402

from snapred.backend.dao.Hook import Hook, hookArgumentsOfType  # noqa: E402
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord  # noqa: E402
from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402
from snapred.meta.Config import Config, Resource  # noqa: E402
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName  # noqa: E402

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.mkdtemp(prefix="hookWorkspaces_"))
root.mkdir(parents=True, exist_ok=True)
fakeInstrument = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")

# --- workspaces of each kind -------------------------------------------------------------------------------
LoadEmptyInstrument(Filename=fakeInstrument, OutputWorkspace="hook_matrix")
createCompatibleMask("hook_mask", "hook_matrix")
CreateGroupingWorkspace(OutputWorkspace="hook_grouping", CustomGroupingString="1", InstrumentFilename=fakeInstrument)
createCompatibleDiffCalTable("hook_table", "hook_matrix")
CreateSampleWorkspace(OutputWorkspace="hook_events", WorkspaceType="Event", NumBanks=1, BankPixelWidth=2)


def hookFunction(**kwargs):
    pass


hooks = {
    "BeforeSomething": [
        Hook(
            func=hookFunction,
            mask=WorkspaceName("hook_mask"),
            label="hook_matrix",  # plain string: must NOT be saved
            n=3,
        ),
    ],
    "AfterSomething": [
        Hook(
            func=hookFunction,
            inputs=[WorkspaceName("hook_grouping"), WorkspaceName("hook_mask")],  # duplicate mask: saved once
            options={"table": WorkspaceName("hook_table"), "events": WorkspaceName("hook_events")},
            missing=WorkspaceName("hook_does_not_exist"),  # skipped, with a warning
        ),
    ],
}
expected = {"hook_mask.h5", "hook_grouping.h5", "hook_table.nxs.h5", "hook_events.nxs.h5"}

# the test config only logs errors: show the "workspace does not exist" warning
import snapred.backend.data.LocalDataService as LocalDataServiceModule  # noqa: E402

LocalDataServiceModule.logger.setLevel(logging.WARNING)
for handler in LocalDataServiceModule.logger.handlers:
    handler.setLevel(logging.WARNING)

service = LocalDataService()
service.generateStateId = lambda *a, **k: ("stateId", None)  # noqa: ARG005
service.constructCalibrationStateRoot = lambda *a, **k: root / "calibrationState"  # noqa: ARG005
service._constructReductionStateRoot = lambda *a, **k: root / "reductionState"  # noqa: ARG005

# --- write the three record types --------------------------------------------------------------------------
calibration = DAOFactory.calibrationRecord("57514", True, 1)
calibration.hooks = hooks
service.writeCalibrationRecord(calibration)
calibrationDir = service.calibrationIndexer(True, "stateId").versionPath(calibration.version)

normalization = DAOFactory.normalizationRecord()
normalization.hooks = hooks
service.writeNormalizationRecord(normalization)
normalizationDir = service.normalizationIndexer(normalization.useLiteMode, "stateId").versionPath(normalization.version)

with open(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json")) as f:
    reduction = ReductionRecord.model_validate_json(f.read())
reduction.hooks = hooks
service.writeReductionRecord(reduction)
reductionRecordFile = service._constructReductionRecordFilePath(
    reduction.runNumber, reduction.useLiteMode, reduction.timestamp
)

# --- check -------------------------------------------------------------------------------------------------
subdir = Config["hook.workspaces.directory"]
ok = True
for label, recordDir in [
    ("calibration", calibrationDir),
    ("normalization", normalizationDir),
    ("reduction", reductionRecordFile.parent),
]:
    hookDir = recordDir / subdir
    found = {p.name for p in hookDir.iterdir()} if hookDir.exists() else set()
    status = "OK" if found == expected else "FAIL"
    ok &= found == expected
    print(f"[{status}] {label}: {hookDir}")
    for name in sorted(found):
        print(f"         {name}")
    if found != expected:
        print(f"         expected: {sorted(expected)}")

# the read-back reduction record still knows which arguments are workspaces
readBack = service.readReductionRecord(reduction.runNumber, reduction.useLiteMode, reduction.timestamp)
names = sorted(set(hookArgumentsOfType(readBack.hooks, WorkspaceName)))
print(f"read-back record: workspace arguments = {names}")

# the saved files reload, with the right types
hookDir = reductionRecordFile.parent / subdir
for ws in ["hook_mask", "hook_grouping", "hook_table", "hook_events"]:
    DeleteWorkspace(ws)
LoadDiffCal(
    Filename=str(hookDir / "hook_mask.h5"),
    WorkspaceName="reload",
    MakeGroupingWorkspace=False,
    MakeCalWorkspace=False,
    InputWorkspace="hook_matrix",
)
LoadDiffCal(
    Filename=str(hookDir / "hook_grouping.h5"),
    WorkspaceName="reloadG",
    MakeMaskWorkspace=False,
    MakeCalWorkspace=False,
    InputWorkspace="hook_matrix",
)
LoadNexus(Filename=str(hookDir / "hook_table.nxs.h5"), OutputWorkspace="reload_table")
LoadNexus(Filename=str(hookDir / "hook_events.nxs.h5"), OutputWorkspace="reload_events")
for ws in ["reload_mask", "reloadG_group", "reload_table", "reload_events"]:
    print(f"reloaded {ws}: {type(mtd[ws]).__name__}")

print(f"\n{'ALL OK' if ok else 'FAILURES'} -- output left in {root}")
sys.exit(0 if ok else 1)

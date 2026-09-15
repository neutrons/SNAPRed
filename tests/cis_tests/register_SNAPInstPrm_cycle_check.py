"""
CIS script: registering a beam cycle must not move any run to a different state.

`SNAPInstPrm` carries `stateIdSchema`, which fixes which PVs are hashed into a run's
state ID -- and therefore which state folder it resolves to and which calibrations it
can see. `updateInstrumentConfigCycle` used to write an open-ended `appliesTo`, so a
newly registered cycle overrode every later configuration epoch and silently moved
those runs. This checks that it no longer does.

It also checks the other half: a cycle now stops at its own last run. SNAP collects
runs after a cycle ends -- beam not yet stable, configuration tests -- and those belong
to no cycle. Only an entry bounded above leaves them that way.

SAFETY: this copies the production SNAPInstPrm to a scratch directory and points
`Config` at the copy. Production is read once and never written.

Run with `pixi run python <this file>`.
"""

import shutil
import sys
import tempfile
from pathlib import Path

import snapred

SNAPRed_module_root = Path(snapred.__file__).parent.parent
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / "tests"))
from util.Config_helpers import Config_override  # noqa: E402

from snapred.backend.dao.state.Cycle import Cycle  # noqa: E402
from snapred.backend.data.DataFactoryService import DataFactoryService  # noqa: E402
from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402
from snapred.meta.Config import Config  # noqa: E402

# --- User input ---
# The cycle to register. 2025-A is the first one awaiting registration. Its last run is
# measured from the production runs: 66474 is the last diffraction production run before
# a 79-day beam gap, two months before 2025-B opens.
CYCLE = Cycle(cycleID="2025-A", startDate="2025-01-28", stopDate="2025-07-01", firstRun=63978, lastRun=66474)
AUTHOR = "cis test"
# Runs spanning the index, chosen to sit either side of every boundary, of the cycle's own
# bounds, and inside the out-of-cycle gap that follows it (66475 to 66568).
PROBE_RUNS = [
    "46342",
    "50000",
    "63977",
    "63978",
    "66474",
    "66475",
    "66556",
    "66557",
    "66569",
    "70000",
    "80000",
]

source = Path(Config["instrument.parameters.home"])
print(f"reading production SNAPInstPrm from : {source}")

with tempfile.TemporaryDirectory(prefix="cis_SNAPInstPrm_") as scratch:
    scratchHome = Path(scratch) / "SNAPInstPrm"
    shutil.copytree(source, scratchHome)
    print(f"working on scratch copy           : {scratchHome}\n")

    with Config_override("instrument.parameters.home", str(scratchHome)):
        service = LocalDataService()

        def survey():
            """Resolved index version and state-ID schema for each probe run."""
            out = {}
            for run in PROBE_RUNS:
                # NB the resolved *index* version, not `cfg.version`: some production
                # SNAPInstPrm.json files carry an internal `version` field that disagrees
                # with their index entry (v_0002 says 1), which is not what resolves.
                version = service.instrumentParameterIndexer().latestApplicableVersion(run)
                try:
                    cfg = service.readInstrumentParameters(run)
                    out[run] = (version, repr(cfg.stateIdSchema), cfg.cycle)
                except FileNotFoundError:
                    out[run] = (version, None, None)
            return out

        def showIndex(label):
            indexer = service.instrumentParameterIndexer()
            print(f"--- index {label} ---")
            for version in sorted(indexer.index):
                e = indexer.index[version]
                print(f"    v{version}: appliesTo={e.appliesTo!r:<22} timestamp={e.timestamp}")
            print()

        showIndex("BEFORE")
        before = survey()

        print(f"registering cycle {CYCLE.cycleID} (firstRun {CYCLE.firstRun}) ...\n")
        DataFactoryService().updateInstrumentConfigCycle(CYCLE, AUTHOR)

        showIndex("AFTER")
        after = survey()

        print(f"{'run':>7}  {'version':>14}  {'cycleID':>10}  state-ID schema")
        moved, tagged = [], []
        for run in PROBE_RUNS:
            vBefore, sBefore, _ = before[run]
            vAfter, sAfter, cAfter = after[run]
            schema = "UNCHANGED" if sAfter == sBefore else "*** MOVED ***"
            if sAfter != sBefore:
                moved.append(run)
            cycleID = cAfter.cycleID if cAfter is not None else "-"
            if cAfter is not None and cAfter.cycleID == CYCLE.cycleID:
                tagged.append(run)
            print(f"{run:>7}  {f'v{vBefore} -> v{vAfter}':>14}  {cycleID:>10}  {schema}")

        expected = [r for r in PROBE_RUNS if CYCLE.runRange.contains(int(r))]
        print()
        print(f"state-ID schema moved for : {moved if moved else 'no runs'}")
        print(f"runs carrying {CYCLE.cycleID:<8}    : {tagged}")
        print(f"expected                  : {expected}")

        assert not moved, f"state-ID schema moved for {moved} -- registration is NOT safe"
        assert tagged == expected, f"cycle tagging wrong: got {tagged}, expected {expected}"
        print(
            "\nPASS: every run kept its state-ID schema, and the cycle reached exactly its own "
            "runs -- nothing below its first run and nothing above its last."
        )

print("\nscratch copy removed; production untouched.")

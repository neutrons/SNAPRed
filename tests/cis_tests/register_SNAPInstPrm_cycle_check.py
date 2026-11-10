"""
CIS script: registering the beam cycles must not move any run to a different state, and
each cycle must reach exactly its own runs -- no further.

`SNAPInstPrm` carries `stateIdSchema`, which fixes which PVs are hashed into a run's state
ID -- and therefore which state folder it resolves to and which calibrations it can see.
`updateInstrumentConfigCycle` used to write an open-ended `appliesTo`, so a newly registered
cycle overrode every later configuration epoch and silently moved those runs. This checks
that it no longer does.

It also checks the other half: a cycle stops at its own last run. SNAP collects runs after a
cycle ends -- beam not yet stable, configuration tests, calibration campaigns that were
later redone -- and those belong to no cycle. Only an entry bounded above leaves them that
way, and only a per-cycle entry keeps the gaps between cycles empty.

WHY EACH CYCLE ENTRY SITS *INSIDE* AN EPOCH ENTRY RATHER THAN REPLACING IT
    The epoch entries (v0..v3 in production) partition run space by instrument
    configuration. A cycle entry is a narrower overlay on top of one: same body, same
    `stateIdSchema`, plus a `cycleID`. Runs inside the cycle resolve to it; runs inside the
    epoch but outside every cycle fall through to the epoch entry, which carries no cycle,
    and so read as out-of-cycle. That fall-through is the entire point -- it is what lets
    the index express "no cycle applies", which a bounded-below-only entry cannot. If a
    cycle entry covered its whole epoch instead, the between-cycle runs would inherit a
    cycle they do not belong to. The table below shows each boundary from both sides.

SAFETY: this copies the production SNAPInstPrm to a scratch directory and points `Config`
at the copy. Production is read once and never written.

Run with `pixi run python <this file>`.
"""

import json
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
# Every completed cycle, from the signed-off boundary table: the first five columns of
# ~/docs/cycleDates_final_review.ods, snapshotted as ~/docs/stage1/cycle_definitive.csv.
# firstRun is reconciled with the IS's formal values (which exist from 2025-A on); lastRun
# is measured from the production runs and hand-checked. 2026-B is the current open cycle
# and is deliberately left out: an open cycle has no upper bound, which is stage 2.
CYCLES = [
    Cycle(cycleID="2022-A", startDate="2022-01-04", stopDate="2022-02-20", firstRun=52862, lastRun=54154),
    Cycle(cycleID="2022-B", startDate="2022-05-25", stopDate="2022-08-08", firstRun=55225, lastRun=57313),
    Cycle(cycleID="2023-A", startDate="2022-11-30", stopDate="2023-02-21", firstRun=57463, lastRun=58554),
    Cycle(cycleID="2023-B", startDate="2023-06-13", stopDate="2023-08-16", firstRun=58808, lastRun=61241),
    Cycle(cycleID="2024-A", startDate="2024-07-10", stopDate="2024-11-22", firstRun=61325, lastRun=63964),
    Cycle(cycleID="2025-A", startDate="2025-01-28", stopDate="2025-05-28", firstRun=63978, lastRun=66474),
    Cycle(cycleID="2025-B", startDate="2025-08-19", stopDate="2025-11-22", firstRun=66569, lastRun=68861),
    Cycle(cycleID="2026-A", startDate="2026-03-04", stopDate="2026-06-24", firstRun=68885, lastRun=72354),
]
AUTHOR = "cis test"

# Probe both sides of every cycle bound, so each boundary is checked from inside and out,
# plus the epoch boundaries and runs off each end of the index.
PROBE_RUNS = sorted(
    {"36610", "46342", "50000", "63976", "63977", "66556", "66557", "80000"}
    | {str(r) for c in CYCLES for r in (c.firstRun - 1, c.firstRun, c.lastRun, c.lastRun + 1)}
)


# Every cycle bound, for the state-ID check: the thing the schema actually controls.
STATE_ID_RUNS = [str(r) for c in CYCLES for r in (c.firstRun, c.lastRun)]


def expectedCycle(run: str):
    """The cycle a run belongs to, or None if it falls between cycles."""
    return next((c.cycleID for c in CYCLES if c.runRange.contains(int(run))), None)


source = Path(Config["instrument.parameters.home"])
print(f"reading production SNAPInstPrm from : {source}")

with tempfile.TemporaryDirectory(prefix="cis_SNAPInstPrm_") as scratch:
    scratchHome = Path(scratch) / "SNAPInstPrm"
    shutil.copytree(source, scratchHome)
    print(f"working on scratch copy           : {scratchHome}\n")

    with Config_override("instrument.parameters.home", str(scratchHome)):
        service = LocalDataService()

        def canonical(schema):
            """Order-insensitive form of a state-ID schema.

            NOT `repr`. A schema read from a file that omits `stateIdSchema` (v_0000 does)
            comes back with enum members where a schema round-tripped through JSON has
            plain ints -- `<_LegacyGuideStatePos.IN: 1>` against `1`. They are `==` equal
            and hash to the same state ID, but their reprs differ, so a string comparison
            reports a move that has not happened. This bit: comparing by repr made the
            v0-epoch cycles look unsafe when they are not.
            """
            return None if schema is None else json.dumps(schema, sort_keys=True, default=str)

        def survey():
            """Resolved index version, state-ID schema and cycle for each probe run."""
            out = {}
            for run in PROBE_RUNS:
                # NB the resolved *index* version, not `cfg.version`: some production
                # SNAPInstPrm.json files carry an internal `version` field that disagrees
                # with their index entry (v_0002 says 1), which is not what resolves.
                version = service.instrumentParameterIndexer().latestApplicableVersion(run)
                try:
                    cfg = service.readInstrumentParameters(run)
                    cycle = cfg.cycle.cycleID if cfg.cycle is not None else None
                    out[run] = (version, canonical(cfg.stateIdSchema), cycle)
                except FileNotFoundError:
                    out[run] = (version, None, None)
            return out

        def stateIds():
            """The real state ID for each cycle bound -- what the schema actually controls.

            The schema comparison above is a proxy; this is the thing itself. Reads run
            metadata, so it is kept to the cycle bounds rather than every probe.
            """
            out = {}
            for run in STATE_ID_RUNS:
                try:
                    out[run] = service.generateStateId(run)[0]
                except Exception as e:  # noqa: BLE001 -- report, do not mask
                    out[run] = f"ERROR {type(e).__name__}"
            return out

        def showIndex(label):
            indexer = service.instrumentParameterIndexer()
            print(f"--- index {label} ---")
            for version in sorted(indexer.index):
                e = indexer.index[version]
                print(f"    v{version}: appliesTo={e.appliesTo!r:<22} timestamp={e.timestamp}")
            print()

        showIndex("BEFORE")
        versionsBefore = set(service.instrumentParameterIndexer().index)
        before = survey()
        stateBefore = stateIds()

        print(f"registering {len(CYCLES)} cycles: {', '.join(c.cycleID for c in CYCLES)}\n")
        for cycle in CYCLES:
            DataFactoryService().updateInstrumentConfigCycle(cycle, AUTHOR)

        showIndex("AFTER")
        after = survey()
        stateAfter = stateIds()

        # Each new entry is narrower than the epoch entry it sits inside. Show that
        # explicitly -- it is the part that looks odd until the fall-through is visible.
        indexer = service.instrumentParameterIndexer()
        epochs = sorted(versionsBefore)
        print("--- each cycle entry is a narrower overlay on the epoch that governs it ---")
        for v in sorted(set(indexer.index) - versionsBefore):
            e = indexer.index[v]
            firstRun = str(next(c.firstRun for c in CYCLES if f">={c.firstRun}," in e.appliesTo))
            host = next(
                (indexer.index[h].appliesTo for h in epochs if indexer._isApplicableEntry(indexer.index[h], firstRun)),
                "?",
            )
            print(f"    v{v:<3} {e.appliesTo!r:<22} sits inside epoch {host!r}")
        print("    -- runs inside an epoch but outside every cycle fall through to the epoch")
        print("       entry, which carries no cycle. That is how 'no cycle applies' is said.")
        print()

        print(
            f"{'run':>7}  {'version':>14}  {'cycle before':>13}  {'cycle after':>12}  {'expected':>10}  state-ID schema"
        )
        moved, misTagged = [], []
        for run in PROBE_RUNS:
            vBefore, sBefore, cBefore = before[run]
            vAfter, sAfter, cAfter = after[run]
            schema = "UNCHANGED" if sAfter == sBefore else "*** MOVED ***"
            if sAfter != sBefore:
                moved.append(run)
            want = expectedCycle(run)
            if cAfter != want:
                misTagged.append((run, cAfter, want))
            flag = "" if cAfter == want else "   <-- WRONG"
            print(
                f"{run:>7}  {f'v{vBefore} -> v{vAfter}':>14}  {str(cBefore or '-'):>13}  "
                f"{str(cAfter or '-'):>12}  {str(want or '-'):>10}  {schema}{flag}"
            )

        movedState = [r for r in STATE_ID_RUNS if stateAfter[r] != stateBefore[r]]
        print()
        print(f"{'run':>7}  {'state ID before':>18}  {'state ID after':>18}")
        for r in STATE_ID_RUNS:
            mark = "" if stateAfter[r] == stateBefore[r] else "   *** MOVED ***"
            print(f"{r:>7}  {stateBefore[r]:>18}  {stateAfter[r]:>18}{mark}")

        print()
        print(f"state-ID schema moved for : {moved if moved else 'no runs'}")
        print(f"state ID moved for        : {movedState if movedState else 'no runs'}")
        print(f"wrongly tagged            : {misTagged if misTagged else 'no runs'}")
        inCycle = [r for r in PROBE_RUNS if expectedCycle(r)]
        outOfCycle = [r for r in PROBE_RUNS if not expectedCycle(r)]
        print(f"probes inside a cycle     : {len(inCycle)}")
        print(f"probes between cycles     : {len(outOfCycle)}  (each must read as '-')")

        assert not moved, f"state-ID schema moved for {moved} -- registration is NOT safe"
        assert not movedState, f"state ID moved for {movedState} -- calibrations would be orphaned"
        assert not misTagged, f"cycle tagging wrong: {misTagged}"
        print(
            "\nPASS: every run kept its state-ID schema AND its actual state ID; every cycle "
            "reached exactly its own runs; and every run between cycles still carries no "
            "cycle at all."
        )

print("\nscratch copy removed; production untouched.")

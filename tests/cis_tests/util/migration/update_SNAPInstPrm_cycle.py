# CIS script: add/update `cycle` info in every SNAPInstPrm.json under the SNAPInstPrm folder.
#
# Each instrument-parameter version lives in its own folder:
#     <instrument.parameters.home>/v_XXXX/SNAPInstPrm.json
# This walks those folders and, for each file, derives the cycle from the file's own
# `indexEntry.appliesTo` run range:
#   * `firstRun` is the lower bound of `appliesTo` (e.g. ">=46342,<=63976" -> 46342),
#     matching how cycles are written elsewhere (DataFactoryService: appliesTo=f">={cycle.firstRun}").
#   * `cycleID` / `startDate` / `stopDate` are SNS facility metadata that cannot be
#     inferred from a run number, so they are looked up from CYCLE_METADATA below.
#
# Every other field in the JSON is preserved. Before any file is overwritten, the
# entire SNAPInstPrm directory is copied to a timestamped backup alongside it; the
# script prints where the backup is and never deletes it -- removing it is left to you.
#
# Edit CYCLE_METADATA below, then run with `pixi run python <this file>`.

import json
import shutil
import time
from pathlib import Path

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.state.Cycle import Cycle
from snapred.backend.dao.state.InstrumentConfig import InstrumentConfig
from snapred.meta.Config import Config

# ----------------------------------------------------------------------------- config

# Set to True to preview changes without touching any files.
DRY_RUN = True

# Overwrite the cycle even if the file already has one. When False, files that
# already contain a `cycle` are left untouched.
OVERWRITE_EXISTING = False

# SNS facility cycle metadata, keyed by the cycle's first run number. The `firstRun`
# derived from each file's `appliesTo` lower bound is matched to the entry with the
# greatest firstRun that is <= the derived run (i.e. the cycle that run belongs to).
# Add the real cycles here.
CYCLE_METADATA = {
    36610: {"cycleID": "2024-A", "startDate": "2024-01-01", "stopDate": "2024-06-30"},
    # 63977: {"cycleID": "2024-B", "startDate": "2024-07-01", "stopDate": "2024-12-31"},
}

def deriveFirstRun(appliesTo: str) -> int:
    """Return the lower-bound run number from an `appliesTo` expression (the cycle's firstRun)."""
    if not appliesTo:
        raise ValueError("indexEntry.appliesTo is missing/empty; cannot derive firstRun")
    lowerBounds = [int(run) for symbol, run in IndexEntry.parseAppliesTo(appliesTo) if symbol in (">=", ">")]
    if not lowerBounds:
        raise ValueError(f"appliesTo '{appliesTo}' has no lower bound (>= or >); cannot derive firstRun")
    return min(lowerBounds)

def resolveCycle(firstRun: int) -> Cycle:
    """Build the Cycle for a derived firstRun, looking up facility metadata from CYCLE_METADATA."""
    candidates = [start for start in CYCLE_METADATA if start <= firstRun]
    if not candidates:
        raise KeyError(
            f"No CYCLE_METADATA entry covers firstRun={firstRun}. "
            f"Add a cycle whose first run is <= {firstRun}."
        )
    meta = CYCLE_METADATA[max(candidates)]
    return Cycle(firstRun=firstRun, **meta)

home = Path(Config["instrument.parameters.home"])
print(f"SNAPInstPrm home: {home}")
if not home.is_dir():
    raise FileNotFoundError(f"SNAPInstPrm home directory does not exist: {home}")

jsonFiles = sorted(home.glob("v_*/SNAPInstPrm.json"))
if not jsonFiles:
    raise FileNotFoundError(f"No 'v_*/SNAPInstPrm.json' files found under {home}")

print(f"Found {len(jsonFiles)} SNAPInstPrm.json file(s).\n")

updated, skipped = 0, 0
backupDir = None
for jsonFile in jsonFiles:
    versionFolder = jsonFile.parent.name

    with open(jsonFile) as f:
        data = json.load(f)

    existing = data.get("cycle")
    if existing is not None and not OVERWRITE_EXISTING:
        print(f"[skip]   {versionFolder}: already has cycle={existing}")
        skipped += 1
        continue

    appliesTo = data.get("indexEntry", {}).get("appliesTo")
    firstRun = deriveFirstRun(appliesTo)
    cycle = resolveCycle(firstRun)

    # Insert/replace the cycle, keeping it just before `indexEntry` to match existing files.
    newData = {}
    inserted = False
    for key, value in data.items():
        if key == "indexEntry" and not inserted:
            newData["cycle"] = cycle.model_dump()
            inserted = True
        if key == "cycle":
            continue  # drop any old cycle; re-added in canonical position
        newData[key] = value
    if not inserted:
        newData["cycle"] = cycle.model_dump()

    # Validate the result round-trips through the DAO before writing.
    InstrumentConfig(**newData)

    action = "would update" if DRY_RUN else "update"
    print(f"[{action}] {versionFolder}: appliesTo='{appliesTo}' -> cycle={cycle.model_dump()}")

    if not DRY_RUN:
        # Back up the entire directory once, before overwriting the first file, so the
        # whole run is reversible. The backup is left in place for you to delete.
        if backupDir is None:
            backupDir = home.parent / f"backup_SNAPInstPrm_{time.strftime('%Y%m%d_%H%M%S')}"
            if backupDir.exists():
                raise FileExistsError(f"Backup directory already exists: {backupDir}")
            print(f"Backing up {home} to {backupDir} ...")
            shutil.copytree(home, backupDir)
            print(f"Backup complete: {backupDir}\n")
        with open(jsonFile, "w") as f:
            json.dump(newData, f, indent=4)
    updated += 1

print(f"\nDone. {updated} file(s) {'to update' if DRY_RUN else 'updated'}, {skipped} skipped.")
if DRY_RUN:
    print("DRY_RUN is True -- no files were written. Set DRY_RUN = False to apply.")
elif backupDir is not None:
    print("\n" + "=" * 72)
    print(f"A backup of the original SNAPInstPrm directory is kept at:\n\n    {backupDir}\n")
    print("It has NOT been deleted. Once you have verified the update succeeded,")
    print("delete the backup yourself to reclaim the space.")
    print("=" * 72)

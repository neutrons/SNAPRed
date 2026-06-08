"""CIS script to test GroupingService.filter{Calibration,Normalization}RecordsByNonStraddlingGroups"""

from pathlib import Path
import sys

import snapred

SNAPRed_module_root = Path(snapred.__file__).parent.parent

sys.path.insert(0, str(Path(SNAPRed_module_root).parent / "tests"))
from util.IPTS_override import IPTS_override

from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.GroupingService import GroupingService


# --- User input ---
runNumber = "58882"
useLiteMode = False
recordType = "calibration"  # "calibration" or "normalization"

with IPTS_override():
    dfs = DataFactoryService()
    gs = GroupingService()

    # Build state ID for this run
    stateId, _ = dfs.constructStateId(runNumber)
    print(f"State ID: {stateId}")

    # Map record type to the relevant functions and group name accessor
    config = {
        "calibration": {
            "exists": dfs.calibrationExists,
            "getIndex": dfs.getCalibrationIndex,
            "readRecord": dfs.lookupService.readCalibrationRecord,
            "filterRecords": gs.filterCalibrationRecordsByNonStraddlingGroups,
            "groupName": lambda rec: rec.focusGroupCalibrationMetrics.focusGroupName,
        },
        "normalization": {
            "exists": dfs.normalizationExists,
            "getIndex": dfs.getNormalizationIndex,
            "readRecord": dfs.lookupService.readNormalizationRecord,
            "filterRecords": gs.filterNormalizationRecordsByNonStraddlingGroups,
            "groupName": lambda rec: rec.calculationParameters.name,
        },
    }

    if recordType not in config:
        raise ValueError(f"Unknown recordType: '{recordType}'. Must be 'calibration' or 'normalization'.")

    cfg = config[recordType]

    if not cfg["exists"](runNumber, useLiteMode, stateId):
        print(f"No {recordType} found for run {runNumber}, state {stateId}. Exiting.")

    index = cfg["getIndex"](useLiteMode, stateId)
    print(f"Found {len(index)} {recordType} index entries")

    # Load records for all versions
    records = []
    for entry in index:
        try:
            record = cfg["readRecord"](
                runId=runNumber, useLiteMode=useLiteMode, state=stateId, version=entry.version
            )
            records.append(record)
            print(
                f"  Loaded record: run={record.runNumber}, version={record.version}, "
                f"focusGroup={cfg['groupName'](record)}"
            )
        except Exception as e:
            print(f"  Skipping entry version {entry.version}: {e}")

    if not records:
        print(f"No {recordType} records loaded. Exiting.")

    print(f"\nTotal records before filtering: {len(records)}")

    filtered = cfg["filterRecords"](records=records, runId=runNumber, useLiteMode=useLiteMode)

    print(f"Total records after filtering: {len(filtered)}")
    excluded = len(records) - len(filtered)
    print(f"Excluded {excluded} record(s) due to East/West panel straddling")

    for rec in filtered:
        print(f"  KEPT: run={rec.runNumber}, version={rec.version}, group={cfg['groupName'](rec)}")



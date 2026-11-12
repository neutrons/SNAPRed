# CIS check: a split cycle's ID ("2026-B.1") is a valid cycle, in a Cycle and in the
# instrument parameters that carry it. READ-ONLY: nothing is written to the
# calibration home, so it is safe to run against any home, production included.
#
# Run from Workbench, or from a shell with SNAPRed's environment active.

import re

from pydantic import ValidationError

from snapred.backend.dao.state.Cycle import Cycle
from snapred.backend.dao.state.InstrumentConfig import InstrumentConfig
from snapred.backend.data.LocalDataService import LocalDataService

# 1. The pattern: splits are accepted, malformed IDs are still refused.
for good in ["2026-B", "2026-B.1", "2026-B.9"]:
    Cycle(cycleID=good, startDate="2026-05-01", stopDate="2026-08-01", firstRun=1)
    print(f"accepted: {good}")
for bad in ["2026-B.0", "2026-B.10", "2026-B.", "2026-b.1"]:
    try:
        Cycle(cycleID=bad, startDate="2026-05-01", stopDate="2026-08-01", firstRun=1)
    except ValidationError:
        print(f"refused:  {bad}")
    else:
        raise AssertionError(f"{bad!r} should have been refused")

# 2. Splits sort between their cycle and the next one.
ids = ["2026-C", "2026-B.1", "2026-B"]
assert sorted(ids) == ["2026-B", "2026-B.1", "2026-C"], sorted(ids)
assert not re.match(Cycle.CYCLE_ID_PATTERN, Cycle.NO_CYCLE)
print("ordering: 2026-B < 2026-B.1 < 2026-C; the 'unknown' sentinel still never matches")

# 3. Instrument parameters carrying a split cycle survive a JSON round trip, which is
#    how they are stored. Takes the newest stored version and relabels its cycle in
#    memory only.
lds = LocalDataService()
indexer = lds.instrumentParameterIndexer()
version = max(e.version for e in indexer.getIndex())
config = lds.readInstrumentParametersVersion(version)
if config.cycle is None:
    print(f"SNAPInstPrm v{version} carries no cycle; step 3 skipped")
else:
    split = config.cycle.model_copy(update={"cycleID": config.cycle.cycleID[:6] + ".1"})
    relabelled = config.model_copy(update={"cycle": split})
    back = InstrumentConfig.model_validate_json(relabelled.model_dump_json())
    assert back.cycle.cycleID == split.cycleID
    print(f"SNAPInstPrm v{version}: {config.cycle.cycleID} relabelled {back.cycle.cycleID} round-trips")

print("PASS")

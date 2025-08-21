import time
from datetime import datetime, timezone

import numpy as np


def timestamp(ensureUnique: bool = False) -> float:
    # no args in astimezone() means local timezone
    nextTimestamp = datetime.now(timezone.utc).astimezone().timestamp()
    if ensureUnique:
        _previousTimestamp = getattr(timestamp, "_previousTimestamp", None)
        if _previousTimestamp is not None:
            # compare as `time.struct_time` to ensure uniqueness after formatting
            if nextTimestamp < _previousTimestamp or time.gmtime(nextTimestamp) == time.gmtime(_previousTimestamp):
                nextTimestamp = _previousTimestamp + 1.0
        timestamp._previousTimestamp = nextTimestamp
    return nextTimestamp


def parseTimestamp(ts: float | str | int) -> float:
    if isinstance(ts, str):
        # Convert ISO format string to timestamp
        return np.datetime64(ts).astype(int) / 1e9  # convert to seconds
    if isinstance(ts, int):
        # DEPRECIATED: support legacy integer encoding
        return float(ts) / 1000.0
    if not isinstance(ts, float):
        raise ValueError("Timestamp must be a float, int, or ISO format string")
    return float(ts)


def isoFromTimestamp(ts: float) -> str:
    # Convert float timestamp (seconds) to integer nanoseconds
    ts_ns = int(ts * 1e9)
    npDatetime = np.datetime64(ts_ns, "ns")
    iso = np.datetime_as_string(npDatetime, timezone="local", unit="ns")
    return iso

import time

import numpy as np
import warnings


def timestamp(ensureUnique: bool = False) -> float:
    # no args in astimezone() means local timezone
    nextTimestamp = time.time_ns() / 1e9  # convert to seconds
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
        # Convert string to timestamp -- the strings into this method are not always ISO format
        # NOTE np.datetime64 throws an obnoxious warning if there is a timezone offset
        # the warning is purely a nuissance and can be ignored
        # note there is an alternative solutiob using the python-dateutil library to handle strings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=Warning)
            time = np.datetime64(ts).astype(int) / 1e9  # convert to seconds
        return time
    if isinstance(ts, int):
        # DEPRECIATED: support legacy integer encoding
        return float(ts) / 1000.0
    if not isinstance(ts, float):
        raise ValueError("Timestamp must be a float, int, or ISO format string")
    return float(ts)


def isoFromTimestamp(ts: float) -> str:
    # Convert float timestamp (seconds) to integer nanoseconds
    ts_ns = int(ts * 1e9)
    # NOTE np.datetime64 throws an obnoxious warning if there is a timezone offset
    # it likely does not occur in this method, but it is safe to ignore it just in case
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=Warning)
        npDatetime = np.datetime64(ts_ns, "ns")
    iso = np.datetime_as_string(npDatetime, timezone="local", unit="ns")
    return iso

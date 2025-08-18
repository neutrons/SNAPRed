import datetime

import numpy as np
import pytz


def timestamp():
    # EST timezone
    easternTimezone = pytz.timezone("US/Eastern")
    return datetime.datetime.now(easternTimezone).timestamp()


def isoFromTimestamp(ts: float) -> str:
    easternTimezone = pytz.timezone("US/Eastern")

    # Convert float timestamp (seconds) to integer nanoseconds
    ts_ns = int(ts * 1e9)
    npDatetime = np.datetime64(ts_ns, "ns")
    iso = np.datetime_as_string(npDatetime, timezone=easternTimezone, unit="ns")
    return iso

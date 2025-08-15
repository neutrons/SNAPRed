import datetime

import pytz


def timestamp():
    # EST timezone
    easternTimezone = pytz.timezone("US/Eastern")
    return datetime.datetime.now(easternTimezone).timestamp()


def isoFromTimestamp(ts):
    easternTimezone = pytz.timezone("US/Eastern")
    return datetime.datetime.fromtimestamp(ts, tz=easternTimezone).isoformat()

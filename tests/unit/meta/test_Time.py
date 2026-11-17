import time
from unittest import TestCase

import pytest

from snapred.meta.Time import isoFromTimestamp, parseTimestamp, timestamp


class TestTime(TestCase):
    def test_timestamp(self):
        t1 = timestamp()
        iso = isoFromTimestamp(t1)
        t2 = parseTimestamp(iso)
        assert t1 == t2

    def test_timestamp_order(self):
        t1 = timestamp()
        t2 = timestamp()
        assert t2 >= t1

    def test_timestamp_ensureUnique(self):
        t1 = timestamp(ensureUnique=True)
        t2 = timestamp(ensureUnique=True)
        assert t2 > t1

    def test_parseTimestamp(self):
        ts = "2024-06-01T03:34:56.789011968-0400"
        expected = 1717227296.789012
        assert parseTimestamp(ts) == expected

    def test_parseTimestamp_error(self):
        with pytest.raises(ValueError, match="Timestamp must be a float, int, or ISO format string"):
            parseTimestamp(None)
        with pytest.raises(ValueError, match="Error parsing datetime string"):
            parseTimestamp("invalid timestamp")
        obj = {"x": 2}
        with pytest.raises(ValueError, match="Timestamp must be a float, int, or ISO format string"):
            parseTimestamp(obj)

    def test_isoFromTimestamp(self):
        ts = 1717227296.789012
        # Use the local UTC offset *at the timestamp*, not the current one:
        #   otherwise the expected offset is wrong whenever daylight-saving time differs between now and then.
        localTime = time.localtime(ts)
        expected = time.strftime("%Y-%m-%dT%H:%M:%S", localTime) + ".789011968" + time.strftime("%z", localTime)
        assert isoFromTimestamp(ts) == expected

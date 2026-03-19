from unittest import TestCase

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

    def test_isoFromTimestamp(self):
        ts = 1717227296.789012
        expected = "2024-06-01T03:34:56.789011968-0400"
        assert isoFromTimestamp(ts) == expected

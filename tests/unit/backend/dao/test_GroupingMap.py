import unittest

from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.meta.Config import Resource


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        groupingMap = GroupingMap.parse_raw(Resource.read("/inputs/SampleGroupingFile.json"))
        # this prints out the FocusGroup in the lite map containing the Name all
        print(groupingMap.lite["All"])
        print(groupingMap.SHA)

    def test_something(self):
        # this makes the test fail so we can see the output
        assert False

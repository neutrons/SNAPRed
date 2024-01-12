from unittest.mock import patch
from pathlib import Path

import pytest
from snapred.backend.dao.state import _GroupingMap
from snapred.meta.Config import Resource

GroupingMap = _GroupingMap.GroupingMap


class TestGroupingMap:
    # Validator test: grouping-file doesn't exist
    @patch.object(_GroupingMap, "logger")
    def test_file_does_not_exist(self, logger):
        GroupingMap.parse_obj(
            {
                "stateId": "deadbeef00000001",
                "liteFocusGroups": [
                    {"name": "Column", "definition": "invalid"},
                ],
                "nativeFocusGroups": [
                    {"name": "All", "definition": "invalid"},
                ],
            }
        )
        #This for loop checks if the warning messages contain a string
        warnCount = 0
        for call in logger.warning.call_args_list:
            if "not found" in call.args[0]:
                warnCount += 1
        assert warnCount > 0


    # Validator test: grouping-file is not actually a grouping file
    @patch.object(_GroupingMap, "logger")
    def test_file_wrong_format(self, logger):
        t=GroupingMap.parse_obj(
            {
                "stateId": "deadbeef00000002",
                "liteFocusGroups": [{"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}],
                "nativeFocusGroups": [
                    {"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}
                ],
            }
        )
        logger.warning.assert_any_call("File format for:tests/resources/inputs/calibration/input.json is wrong")

    # Validator test: no grouping files are listed in the JSON
    @patch.object(_GroupingMap, "logger")
    def test_empty_list(self, logger):
        mockGroupingMap = GroupingMap.parse_obj(
            {"stateId": "deadbeef00000003", "liteFocusGroups": [], "nativeFocusGroups": []}
        )
        assert len(mockGroupingMap.lite)==0
        logger.warning.assert_any_call("No valid FocusGroups given for mode: lite")
        logger.warning.assert_any_call("No valid FocusGroups given for mode: native")

    # Validator test: relative vs absolute paths
    @patch.object(GroupingMap, "getPath")
    @patch.object(_GroupingMap, "logger")
    def test_relative_vs_absolute_path(self, logger, mockGetPath):
        absPath = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml")
        mockGetPath.return_value = Path(Resource.getPath("inputs/pixel_grouping"))

        GroupingMap.parse_obj(
            {
                "stateId": "deadbeef00000004",
                "liteFocusGroups": [{"name": "AbsPath", "definition": absPath}],
                "nativeFocusGroups": [
                    {"name": "RelativePath", "definition": "SNAPFocGroup_Column.xml"}
                ],
            }
        )
        logger.warning.assert_not_called()

from unittest.mock import patch

import pytest
from snapred.backend.dao.state import _GroupingMap

GroupingMap = _GroupingMap.GroupingMap


class TestGroupingMap:
    # Validator test: grouping-file doesn't exist
    @patch.object(_GroupingMap, "logger")
    def test_file_does_not_exist(self, logger):
        GroupingMap.parse_obj(
            {
                "stateID": "1",
                "liteFocusGroups": [
                    {"name": "Column", "definition": "invalid path"},
                    {"name": "All", "definition": "valid path"},
                ],
                "nativeFocusGroups": [
                    {"name": "Column", "definition": "valid path"},
                    {"name": "All", "definition": "invalid path"},
                ],
            }
        )
        logger.warning.assert_called_with("File NOT FOUND")

    # Validator test: grouping-file is not actually a grouping file
    @patch.object(_GroupingMap, "logger")
    def test_file_wrong_format(self, logger):
        GroupingMap.parse_obj(
            {
                "stateID": "2",
                "liteFocusGroups": [{"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}],
                "nativeFocusGroups": [
                    {"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}
                ],
            }
        )
        logger.warning.assert_called_with("not valid file")

    # Validator test: no grouping files are listed in the JSON
    @patch.object(_GroupingMap, "logger")
    def test_empty_list(self, logger):
        mockGroupingMap = GroupingMap.parse_obj({"stateID": "3", "liteFocusGroups": [], "nativeFocusGroups": []})
        assert len(mockGroupingMap.lite) == 0
        logger.warning.assert_called_with("No FocusGroups for lite mode given")

    # Test loading from the default location
    def test_load_default(self):
        groupingMap = GroupingMap.load("default")
        assert groupingMap.stateID == "default"

    # Tests of setting the state ID (from the actual state) and writing to the 'instrument.state.root' location
    def test_save_state(self):
        mockGroupingMap = GroupingMap.parse_obj(
            {
                "stateID": "stateID",
                "liteFocusGroups": [{"name": "Column", "definition": "path"}, {"name": "All", "definition": "path"}],
                "nativeFocusGroups": [{"name": "Column", "definition": "path"}, {"name": "All", "definition": "path"}],
            }
        )
        mockGroupingMap.save("stateID")
        loadedGroupingMap = GroupingMap.load("stateID")
        assert loadedGroupingMap.stateID == "stateID"

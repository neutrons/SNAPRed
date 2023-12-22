from unittest.mock import patch

import pytest
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.meta.Config import Resource


class TestDiffractionCalibtationRecipe:
    def setup_method(self):
        self.groupingMap = GroupingMap.parse_raw(Resource.read("/inputs/SampleGroupingFile.json"))

    # Tests of basic serialization: read / write to and from a JSON file on disk;
    # Validator tests: grouping-file doesn't exist;  correct behavior is to log,
    #  and continue to load (but _omit_ these groupings from the loaded groupingMap);
    @patch("snapred.backend.log.logger")
    def test_file_does_not_exist(self, mockLogger):
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
        # figure out assert call to logger
        mockLogger.assert_called_once()

    # Validator tests: grouping-file is not actually a grouping file; correct behavior is to log and continue to load;
    def test_file_wrong_format(self):
        GroupingMap.parse_obj(
            {
                "stateID": "2",
                "liteFocusGroups": [{"name": "Column", "definition": "wrong file format"}],
                "nativeFocusGroups": [{"name": "Column", "definition": "wrong file format"}],
            }
        )
        # figure out assert call to logger
        assert False

    # Validator tests: no grouping files are listed in the JSON;
    def test_empty_list(self):
        mockGroupingMap = GroupingMap.parse_obj({"stateID": "3", "liteFocusGroups": [], "nativeFocusGroups": []})
        assert len(mockGroupingMap.lite) == 0

    # Tests of loading from the default location, with the <state ID> specified as '_default_';
    # for the moment, use '~/_test_tmp/calibration.grouping.home' as the location, until the SNS-filesystem mock is ready;
    def test_load_default(self):
        groupingMap = GroupingMap.load("default")
        assert groupingMap.stateID == "default"

    # Tests of setting the state ID (from the actual state) and writing to the 'instrument.state.root' location;
    #  for the moment, use '~/_test_tmp/instrument.state.home/<state SHA>'  "   ";
    def test_save_state(self):
        mockGroupingMap = GroupingMap.parse_obj(
            {
                "stateID": "4",
                "liteFocusGroups": [{"name": "Column", "definition": "path"}, {"name": "All", "definition": "path"}],
                "nativeFocusGroups": [{"name": "Column", "definition": "path"}, {"name": "All", "definition": "path"}],
            }
        )
        mockGroupingMap.save("4")
        loadedGroupingMap = GroupingMap.load("4")
        assert loadedGroupingMap.stateID == "4"


# Any logged messages should be helpful for any normal end user, and also for a CIS dealing with this system.
# For example, if there is a JSON-format error, a log message should indicate where the example _default_ version of the file is located, for comparison.

from pathlib import Path

# Important: this is a *pure* pytest module: no `unittest` imports should be used.
import pytest

import logging
from snapred.backend.dao.state import _GroupingMap as GroupingMapModule
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource

GroupingMap = GroupingMapModule.GroupingMap

@pytest.fixture(autouse=True)
def capture_logging(monkeypatch):
    # For some reason pytest 'caplog' doesn't work with the SNAPRed logging setup.  (TODO: fix this!)
    # This patch bypasses the issue, by renaming and patching the `GroupingMap` module's logger to a standard python `Logger`.
    defaultLogger = logging.getLogger(GroupingMapModule.__name__ + '_patch')
    defaultLogger.propagate = True
    monkeypatch.setattr(GroupingMapModule, 'logger', defaultLogger)

class TestGroupingMap:

    # Validator test: logging: if a grouping-file doesn't exist it won't be included in the map; warning messages will be logged. 
    def test_GroupingMap_file_does_not_exist(self, caplog):
        with caplog.at_level(logging.WARNING):
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
        assert 'not found' in caplog.text

    # Validator test: logging: grouping-files in the grouping-schema with invalid formats won't be included in the map; warning messages will be logged.
    def test_GroupingMap_file_incorrect_format(self, caplog):
        with caplog.at_level(logging.WARNING):
            # JSON is not a valid format for grouping files.
            t=GroupingMap.parse_obj(
                {
                    "stateId": "deadbeef00000002",
                    "liteFocusGroups": [{"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}],
                    "nativeFocusGroups": [
                        {"name": "Column", "definition": "tests/resources/inputs/calibration/input.json"}
                    ],
                }
            )
        assert "File format for: \"tests/resources/inputs/calibration/input.json\" is not a valid grouping-schema map format" in caplog.text

    # Validator test: logging: if no grouping files are listed in the grouping-schema map JSON for any given mode; warning messages will be loggged.
    def test_GroupingMap_empty_list(self, caplog):
        with caplog.at_level(logging.WARNING):
            mockGroupingMap = GroupingMap.parse_obj(
                {"stateId": "deadbeef00000003", "liteFocusGroups": [], "nativeFocusGroups": []}
            )
            assert len(mockGroupingMap.lite)==0
            assert len(mockGroupingMap.native)==0
        assert "No valid FocusGroups were specified for mode: \'lite\'" in caplog.text
        assert "No valid FocusGroups were specified for mode: \'native\'" in caplog.text

    # Validator test: relative paths in `FocusGroup` are relative to <instrument.calibration.powder.grouping.home>.
    def test_GroupingMap_relative_path(self, caplog, monkeypatch):
        monkeypatch.setattr(GroupingMap, 'calibrationGroupingHome', lambda : Path(Resource.getPath("inputs/pixel_grouping")))
        with caplog.at_level(logging.WARNING):
            GroupingMap.parse_obj(
                {
                    "stateId": "deadbeef00000004",
                    "liteFocusGroups": [
                        {"name": "RelativePath", "definition": "SNAPFocGroup_Column.xml"}
                    ],
                    "nativeFocusGroups": [
                        {"name": "RelativePath", "definition": "SNAPFocGroup_Column.xml"}
                    ],
                }
            )
        assert len(caplog.text) == 0

    # Validator test: absolute paths in `FocusGroup` are allowed.
    def test_GroupingMap_absolute_path(self, caplog):
        absPath0 = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml")
        absPath1 = Resource.getPath("inputs/pixel_grouping/SNAPFocGroup_Column.xml")
        with caplog.at_level(logging.WARNING):
            GroupingMap.parse_obj(
                {
                    "stateId": "deadbeef00000004",
                    "liteFocusGroups": [
                        {"name": "AbsPath", "definition": absPath0}
                    ],
                    "nativeFocusGroups": [
                        {"name": "AbsPath", "definition": absPath1}
                    ],
                }
            )
        assert len(caplog.text) == 0

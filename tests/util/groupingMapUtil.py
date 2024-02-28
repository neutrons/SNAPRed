import os

import pytest


class GroupingMapTestFactory:
    def badFilePath(self):
        return {
            "stateId": "deadbeef00000001",
            "liteFocusGroups": [
                {"name": "Column", "definition": "invalid"},
            ],
            "nativeFocusGroups": [
                {"name": "All", "definition": "invalid"},
            ],
        }

    def invalidFileFormat(self):
        return {
            "stateId": "deadbeef00000002",
            "liteFocusGroups": [
                {
                    "name": "Column",
                    "definition": os.path.expanduser(
                        "~/SNAPRed/tests/resources/inputs/calibration/ReductionIngredients.json"
                    ),
                }
            ],
            "nativeFocusGroups": [
                {
                    "name": "Column",
                    "definition": os.path.expanduser(
                        "~/SNAPRed/tests/resources/inputs/calibration/ReductionIngredients.json"
                    ),
                }
            ],
        }

    def emptyGroups(self):
        return {"stateId": "deadbeef00000003", "liteFocusGroups": [], "nativeFocusGroups": []}


@pytest.fixture()
def groupingMapFactory():
    return GroupingMapTestFactory()

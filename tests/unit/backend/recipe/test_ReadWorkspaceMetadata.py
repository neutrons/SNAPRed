"""
NOTE DO NOT use WriteWorkspaceMetadata in these tests
because that algorithm depends on ReadWorkspaceMetadata
"""

import unittest
from typing import Dict, Literal
from unittest import mock

import pytest

# mantid imports
from mantid.simpleapi import AddSampleLogMultiple, CreateSingleValuedWorkspace, mtd
from pydantic import BaseModel, Extra, ValidationError

# the algorithm to test
from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.meta.Config import Config
from util.helpers import deleteWorkspaceNoThrow

thisRecipe = "snapred.backend.recipe.ReadWorkspaceMetadata."
TAG_PREFIX = Config["metadata.tagPrefix"]


# this mocks out the original workspace metadata class
# to ensure the algorithm is as generic as possible in case of later extension
class WorkspaceMetadata(BaseModel, extra=Extra.forbid):
    fruit: Literal[UNSET, "apple", "pear", "orange"] = UNSET
    veggie: Literal[UNSET, "broccoli", "cauliflower"] = UNSET
    bread: Literal[UNSET, "sourdough", "pumpernickel"] = UNSET
    dairy: Literal[UNSET, "milk", "cheese", "yogurt", "butter"] = UNSET


# NOTE do NOT remove this patch
# if your test fails with this patch, then fix your test instead
@mock.patch(thisRecipe + "WorkspaceMetadata", WorkspaceMetadata)
class TestReadWorkspaceMetadata(unittest.TestCase):
    properties = list(WorkspaceMetadata.schema()["properties"].keys())
    propLogNames = [TAG_PREFIX + prop for prop in properties]

    def setUp(self):
        self.values = ["apple", "broccoli", "sourdough", "cheese"]
        self.metadata = WorkspaceMetadata.parse_obj(dict(zip(self.properties, self.values)))
        return super().setUp()

    def tearDown(self) -> None:
        """
        Delete all workspaces created by previous tests.
        """
        for ws in mtd.getObjectNames():
            deleteWorkspaceNoThrow(ws)
        del self.metadata
        return super().tearDown()

    def _make_groceries(self, wsname: str = "test_read_metadata") -> Dict[str, str]:
        wsname = mtd.unique_name(5, prefix=wsname)
        assert not mtd.doesExist(wsname)
        CreateSingleValuedWorkspace(OutputWorkspace=wsname, DataValue=2.0)  # NOTE value needed in one test
        assert mtd.doesExist(wsname)
        self._verify_no_logs(wsname)
        return {"workspace": wsname}

    def _verify_no_logs(self, wsname):
        # verify no logs exist on the workspace
        run = mtd[wsname].getRun()
        for prop in self.propLogNames:
            assert not run.hasProperty(prop)
        return {"workspace": wsname}

    def _write_metadata(self, metadata, groceries):
        obj_dict = metadata.dict()
        lognames = [TAG_PREFIX + log for log in obj_dict.keys()]
        logvalues = list(obj_dict.values())
        self._write_logs(lognames, logvalues, groceries)

    def _write_logs(self, lognames, logvalues, groceries):
        AddSampleLogMultiple(
            Workspace=groceries["workspace"],
            LogNames=lognames,
            LogValues=logvalues,
        )
        # verify all the logs exist
        run = mtd[groceries["workspace"]].getRun()
        for value, prop in zip(logvalues, lognames):
            assert run.hasProperty(prop)
            assert value == run.getLogData(prop).value

    def test_read_nothing(self):
        # create a blank workspace with no logs
        groceries = self._make_groceries()

        # now run the recipe to read the logs amd verify entirely unset
        ans = ReadWorkspaceMetadata().cook(groceries)
        ref = WorkspaceMetadata()
        assert ref == ans

    def test_read_metadata(self):
        # create a workspace and set the logs
        groceries = self._make_groceries()
        self._write_metadata(self.metadata, groceries)

        # now run the recipe to read the logs and verify the correct object created
        ans = ReadWorkspaceMetadata().cook(groceries)
        assert ans == self.metadata

    def test_read_all_unset(self):
        # create all the logs in an unset state
        groceries = self._make_groceries()
        blankValues = [UNSET for prop in self.properties]
        self._write_logs(self.propLogNames, blankValues, groceries)

        # now run the recipe to read the logs and verify all unset
        ans = ReadWorkspaceMetadata().cook(groceries)
        ref = WorkspaceMetadata()
        assert ans == ref

    def test_read_one_unset(self):
        NUM = len(self.properties)

        for i in range(NUM):
            groceries = self._make_groceries()
            # unset one of the properties in the metadata
            setattr(self.metadata, self.properties[i], UNSET)
            self._write_metadata(self.metadata, groceries)
            # read the metadata back and verify it is correct
            ans = ReadWorkspaceMetadata().cook(groceries)
            assert ans == self.metadata

            # return self.metadata to its prior condition
            setattr(self.metadata, self.properties[i], self.values[i])

    def test_invalid_workspace(self):
        groceries = {"workspace": "invalid_workspace_name_what_a_dumb_name"}
        with pytest.raises(RuntimeError) as e:
            ReadWorkspaceMetadata().validateInputs({}, groceries)
        assert "ADS" in str(e)

    def test_invalid_dao(self):
        """
        If logs were somehow written with invalid tags,
        make sure this recipe will fail to validate them
        """
        groceries = self._make_groceries()
        # add bad values to the logs
        badValues = ["newt" for prop in self.properties]
        self._write_logs(self.propLogNames, badValues, groceries)

        with pytest.raises(ValidationError) as e:
            ReadWorkspaceMetadata().cook(groceries)
        assert "WorkspaceMetadata" in str(e)


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)

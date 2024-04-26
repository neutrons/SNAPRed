import unittest
from typing import Dict, Literal
from unittest import mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from pydantic import BaseModel, Extra
from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config
from util.helpers import deleteWorkspaceNoThrow

thisService = "snapred.backend.service.WorkspaceMetadataService."
writeRecipe = "snapred.backend.recipe.WriteWorkspaceMetadata."
readRecipe = "snapred.backend.recipe.ReadWorkspaceMetadata."
TAG_PREFIX = Config["metadata.tagPrefix"]


# this mocks out the original workspace metadata class
# to ensure the algorithm is as generic as possible in case of later extension
class WorkspaceMetadata(BaseModel, extra=Extra.forbid):
    fruit: Literal[UNSET, "apple", "pear", "orange"] = UNSET
    veggie: Literal[UNSET, "broccoli", "cauliflower"] = UNSET
    bread: Literal[UNSET, "sourdough", "pumpernickel"] = UNSET
    dairy: Literal[UNSET, "milk", "cheese", "yogurt", "butter"] = UNSET


# NOTE do NOT remove these patches for any reason whatsoever
@mock.patch(thisService + "WorkspaceMetadata", WorkspaceMetadata)
@mock.patch(writeRecipe + "Ingredients", WorkspaceMetadata)
@mock.patch(readRecipe + "Ingredients", WorkspaceMetadata)
class TestMetadataService(unittest.TestCase):
    properties = list(WorkspaceMetadata.schema()["properties"].keys())
    propLogNames = [f"{TAG_PREFIX}{prop}" for prop in properties]

    @classmethod
    def setUpClass(cls):
        cls.instance = WorkspaceMetadataService()
        super().setUpClass()

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
        return super().tearDown()

    def _make_workspace(self, wsname: str = "test_metadata_service") -> str:
        wsname = mtd.unique_name(5, prefix=wsname)
        assert not mtd.doesExist(wsname)
        CreateSingleValuedWorkspace(OutputWorkspace=wsname, DataValue=2.0)  # NOTE value needed in one test
        assert mtd.doesExist(wsname)
        self._verify_no_logs(wsname)
        return wsname

    def _verify_log(self, wsname, prop, value):
        run = mtd[wsname].getRun()
        assert run.hasProperty(TAG_PREFIX + prop)
        assert run.getLogData(TAG_PREFIX + prop).value == value

    def _verify_logs(self, wsname, values):
        # verify the properties exist and have correct values
        run = mtd[wsname].getRun()
        for value, prop in zip(values, self.propLogNames):
            assert run.hasProperty(prop)
            assert value == run.getLogData(prop).value

    def _verify_no_logs(self, wsname):
        # verify no logs exist on the workspace
        run = mtd[wsname].getRun()
        for prop in self.propLogNames:
            assert not run.hasProperty(prop)
        return {"workspace": wsname}

    def test_name(self):
        assert "metadata" == self.instance.name()

    def test_read_nothing(self):
        # create a blank workspace with no logs
        wsname = self._make_workspace()

        # now run the recipe to read the logs amd verify entirely unset
        ans = self.instance.readWorkspaceMetadata(wsname)
        ref = WorkspaceMetadata()
        assert ref == ans

    def test_read_and_write_metadata(self):
        # create a workspace and set the logs
        wsname = self._make_workspace()
        assert self.instance.writeWorkspaceMetadata(wsname, self.metadata)
        self._verify_logs(wsname, self.values)

        ans = self.instance.readWorkspaceMetadata(wsname)
        assert ans == self.metadata

    def test_read_and_write_single(self):
        # create a workspace and set a single log
        for value, prop in zip(self.values, self.properties):
            wsname = self._make_workspace()
            assert self.instance.writeMetadataTag(wsname, prop, value)
            self._verify_log(wsname, prop, value)

            ans = self.instance.readMetadataTag(wsname, prop)
            assert ans == value

    def test_read_and_write_lists(self):
        wsname = self._make_workspace()
        assert self.instance.writeMetadataTags(wsname, self.properties, self.values)
        self._verify_logs(wsname, self.values)

        ans = self.instance.readMetadataTags(wsname, self.properties)
        assert ans == self.values


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

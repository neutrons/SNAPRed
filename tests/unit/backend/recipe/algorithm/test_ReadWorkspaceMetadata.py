"""
NOTE DO NOT use WriteWorkspaceMetadata in these tests
because that algorithm depends on ReadWorkspaceMetadata
"""

import unittest
from typing import Literal
from unittest import mock

import pytest

# mantid imports
from mantid.simpleapi import AddSampleLog, AddSampleLogMultiple, CreateSingleValuedWorkspace, mtd
from pydantic import BaseModel

# the algorithm to test
from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.recipe.algorithm.ReadWorkspaceMetadata import ReadWorkspaceMetadata

thisAlgorithm = "snapred.backend.recipe.algorithm.ReadWorkspaceMetadata."


# this mocks out the original workspace metadata class
# to ensure the algorithm is as generic as possible in case of later extension
class MockWorkspaceMetadata(BaseModel):
    fruit: Literal[UNSET, "apple", "pear", "orange"] = UNSET
    veggie: Literal[UNSET, "broccoli", "cauliflower"] = UNSET
    bread: Literal[UNSET, "sourdough", "pumpernickel"] = UNSET
    dairy: Literal[UNSET, "milk", "cheese", "yogurt", "butter"] = UNSET


class TestReadWorkspaceMetadata(unittest.TestCase):
    properties = list(MockWorkspaceMetadata.schema()["properties"].keys())

    @mock.patch(thisAlgorithm + "WorkspaceMetadata", MockWorkspaceMetadata)
    def test_read_properties(self):
        wsname = "_test_read_ws_metadata"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        assert wsname in mtd
        properties = [f"SNAPRed_{prop}" for prop in self.properties]
        values = ["apple", "broccoli", "sourdough", "cheese"]

        # add the logs to the workspace
        AddSampleLogMultiple(
            Workspace=wsname,
            LogNames=properties,
            LogValues=values,
        )
        # verify the logs exist on the workspace
        run = mtd[wsname].getRun()
        for prop in properties:
            assert run.hasProperty(prop)

        # now run the algorithm to read the logs and verify the correct object created
        algo = ReadWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.execute()

        # verify each property is set to appropriate value
        metadata = MockWorkspaceMetadata.parse_raw(algo.getPropertyValue("WorkspaceMetadata"))
        for value, prop in zip(values, self.properties):
            assert value == getattr(metadata, prop)

        # verify it equals the correct object
        dictionary = dict(zip(self.properties, values))
        ref = MockWorkspaceMetadata.parse_obj(dictionary)
        assert ref == metadata

    @mock.patch(thisAlgorithm + "WorkspaceMetadata", MockWorkspaceMetadata)
    def test_read_unset(self):
        wsname = "_test_read_ws_metadata_unset"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        assert wsname in mtd
        properties = [f"SNAPRed_{prop}" for prop in self.properties]
        values = [UNSET for prop in properties]

        # create all the logs in an unset state
        # add the logs to the workspace
        AddSampleLogMultiple(
            Workspace=wsname,
            LogNames=properties,
            LogValues=values,
        )

        # verify the logs exist on the workspace
        run = mtd[wsname].getRun()
        for prop in properties:
            assert run.getLogData(prop).value == UNSET

        # now run the algorithm to read the logs and verify the correct object created
        algo = ReadWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.execute()
        metadata = MockWorkspaceMetadata.parse_raw(algo.getPropertyValue("WorkspaceMetadata"))

        # ensure it is equal to an object with all fields unset
        assert metadata == MockWorkspaceMetadata()


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

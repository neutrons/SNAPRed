import unittest
from typing import Literal
from unittest import mock

import pytest

# mantid imports
from mantid.simpleapi import (
    AddSampleLog,
    CloneWorkspace,
    CreateSingleValuedWorkspace,
    Power,
    mtd,
)
from pydantic import BaseModel

# the algorithm to test
from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.recipe.algorithm.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.backend.recipe.algorithm.WriteWorkspaceMetadata import WriteWorkspaceMetadata
from util.helpers import deleteWorkspaceNoThrow

thisAlgorithm = "snapred.backend.recipe.algorithm.WriteWorkspaceMetadata."
readAlgorithm = "snapred.backend.recipe.algorithm.ReadWorkspaceMetadata."


# this mocks out the original workspace metadata class
# to ensure the algorithm is as generic as possible in case of later extension
class WorkspaceMetadata(BaseModel):
    fruit: Literal[UNSET, "apple", "pear", "orange"] = UNSET
    veggie: Literal[UNSET, "broccoli", "cauliflower"] = UNSET
    bread: Literal[UNSET, "sourdough", "pumpernickel"] = UNSET
    dairy: Literal[UNSET, "milk", "cheese", "yogurt", "butter"] = UNSET


# NOTE do NOT remove these patches
# if your test fails with these patches, then fix your test instead
@mock.patch(thisAlgorithm + "WorkspaceMetadata", WorkspaceMetadata)
@mock.patch(readAlgorithm + "WorkspaceMetadata", WorkspaceMetadata)
class TestWriteWorkspaceMetadata(unittest.TestCase):
    properties = list(WorkspaceMetadata.schema()["properties"].keys())

    def tearDown(cls) -> None:
        """
        Delete all workspaces created by previous tests.
        """
        for ws in mtd.getObjectNames():
            deleteWorkspaceNoThrow(ws)
        return super().tearDown()

    def test_write_metadata_direct(self):
        """
        This tests writing metadata by examining the workspace directly
        rather than using the ReadWorkspaceMetadata algorithm, for peace of mind.
        """
        wsname = "_test_write_ws_metadata_1"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        assert wsname in mtd
        properties = [f"SNAPRed_{prop}" for prop in self.properties]
        values = ["apple", "broccoli", "sourdough", "cheese"]

        # verify the logs do not exist on the workspace
        run = mtd[wsname].getRun()
        for prop in properties:
            assert not run.hasProperty(prop)

        ref = WorkspaceMetadata.parse_obj(dict(zip(self.properties, values)))

        # now run the algorithm to write the logs
        algo = WriteWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.setProperty("WorkspaceMetadata", ref.json())
        algo.execute()

        run = mtd[wsname].getRun()
        for value, prop in zip(values, properties):
            assert value == run.getLogData(prop).value

    def test_write_metadata(self):
        wsname = "_test_write_ws_metadata_2"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        assert wsname in mtd
        properties = [f"SNAPRed_{prop}" for prop in self.properties]
        values = ["apple", "broccoli", "sourdough", "cheese"]

        # verify the logs do not exist on the workspace
        run = mtd[wsname].getRun()
        for prop in properties:
            assert not run.hasProperty(prop)

        ref = WorkspaceMetadata.parse_obj(dict(zip(self.properties, values)))

        # now run the algorithm to write the logs
        algo = WriteWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.setProperty("WorkspaceMetadata", ref.json())
        algo.execute()

        # read the metadata back
        read = ReadWorkspaceMetadata()
        read.initialize()
        read.setProperty("Workspace", wsname)
        read.execute()

        # verify the metadata object exists and matches what it was set to
        ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
        assert ref == ans

    def test_write_partial(self):
        NUM = len(self.properties)
        all_properties = [f"SNAPRed_{prop}" for prop in self.properties]
        all_values = ["apple", "broccoli", "sourdough", "cheese"]

        for i in range(NUM):
            wsname = f"_test_write_ws_metadata_partial_{i}"
            CreateSingleValuedWorkspace(OutputWorkspace=wsname)
            assert wsname in mtd
            # verify the logs do not exist on the workspace
            run = mtd[wsname].getRun()
            for prop in all_properties:
                assert not run.hasProperty(prop)

            # create a metadata object missing one value
            properties = [self.properties[j] for j in range(NUM) if j != i]
            values = [all_values[j] for j in range(NUM) if j != i]
            ref = WorkspaceMetadata.parse_obj(dict(zip(properties, values)))

            # now run the algorithm to write the logs
            algo = WriteWorkspaceMetadata()
            algo.initialize()
            algo.setProperty("Workspace", wsname)
            algo.setProperty("WorkspaceMetadata", ref.json())
            algo.execute()

            # read the metadata back
            read = ReadWorkspaceMetadata()
            read.initialize()
            read.setProperty("Workspace", wsname)
            read.execute()

            # verify the metadata object exists and the dropped property is unset
            ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
            assert ref == ans
            assert getattr(ans, self.properties[i]) == UNSET

    def test_write_retain(self):
        """
        If the workspace has a property set, and write is called
        with a metadata object having an unset property,
        then retain the set property from the workspace's logs
        """
        NUM = len(self.properties)
        all_properties = [f"SNAPRed_{prop}" for prop in self.properties]
        all_values = ["apple", "broccoli", "sourdough", "cheese"]

        for i in range(NUM):
            # create a workspace with one log already there
            wsname = f"_test_write_ws_metadata_partial_{i}"
            CreateSingleValuedWorkspace(OutputWorkspace=wsname)
            AddSampleLog(
                Workspace=wsname,
                LogName=all_properties[i],
                LogText=all_values[i],
            )
            assert wsname in mtd
            run = mtd[wsname].getRun()
            assert run.hasProperty(all_properties[i])
            assert run.getLogData(all_properties[i]).value == all_values[i]

            # create a metadata object missing that property
            properties = [self.properties[j] for j in range(NUM) if j != i]
            values = [all_values[j] for j in range(NUM) if j != i]
            ref = WorkspaceMetadata.parse_obj(dict(zip(properties, values)))
            assert getattr(ref, self.properties[i]) == UNSET

            # now run the algorithm to write the logs
            algo = WriteWorkspaceMetadata()
            algo.initialize()
            algo.setProperty("Workspace", wsname)
            algo.setProperty("WorkspaceMetadata", ref.json())
            algo.execute()

            # read the metadata back
            read = ReadWorkspaceMetadata()
            read.initialize()
            read.setProperty("Workspace", wsname)
            read.execute()

            # verify the metadata object exists and has the previously set property is set
            ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
            for prop in properties:
                assert getattr(ref, prop) == getattr(ans, prop)
            assert getattr(ref, self.properties[i]) != getattr(ans, self.properties[i])
            assert getattr(ans, self.properties[i]) != UNSET
            # also check directly on the workspace
            assert run.hasProperty(all_properties[i])
            assert run.getLogData(all_properties[i]).value == all_values[i]

    def test_overwrite_prev_setting(self):
        NUM = len(self.properties)
        all_properties = [f"SNAPRed_{prop}" for prop in self.properties]
        all_values = ["apple", "broccoli", "sourdough", "cheese"]
        alt_values = ["pear", "cauliflower", "pumpernickel", "yogurt"]

        metadata = WorkspaceMetadata.parse_obj(dict(zip(self.properties, all_values)))

        for i in range(NUM):
            # create a workspace with one log already there
            wsname = f"_test_write_ws_metadata_overwrite_{i}"
            CreateSingleValuedWorkspace(OutputWorkspace=wsname)
            AddSampleLog(
                Workspace=wsname,
                LogName=all_properties[i],
                LogText=alt_values[i],
            )
            assert wsname in mtd
            run = mtd[wsname].getRun()
            assert run.hasProperty(all_properties[i])
            assert run.getLogData(all_properties[i]).value == alt_values[i]

            # now run the algorithm to write the logs
            algo = WriteWorkspaceMetadata()
            algo.initialize()
            algo.setProperty("Workspace", wsname)
            algo.setProperty("WorkspaceMetadata", metadata.json())
            algo.execute()

            # read the metadata back
            read = ReadWorkspaceMetadata()
            read.initialize()
            read.setProperty("Workspace", wsname)
            read.execute()

            # verify the metadata object exists and has the previously set property is set
            ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
            assert metadata == ans
            # also check directly on the workspace that he value changed
            assert run.hasProperty(all_properties[i])
            assert run.getLogData(all_properties[i]).value == all_values[i]
            assert run.getLogData(all_properties[i]).value != alt_values[i]

    def test_transform_retain_logs(self):
        wsname = "_test_write_transform"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname, DataValue=2)
        values = ["apple", "broccoli", "sourdough", "cheese"]

        ref = WorkspaceMetadata.parse_obj(dict(zip(self.properties, values)))

        # now run the algorithm to write the logs
        algo = WriteWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.setProperty("WorkspaceMetadata", ref.json())
        algo.execute()

        # transform the first workspace
        wsname2 = Power(wsname, 3)
        assert wsname2.dataY(0) != mtd[wsname].dataY(0)

        # read the metadata from trnasformed workspace
        read = ReadWorkspaceMetadata()
        read.initialize()
        read.setProperty("Workspace", wsname2)
        read.execute()

        # verify the metadata object in transformed workspace matches original
        ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
        assert ref == ans

    def test_clone_retain_logs(self):
        wsname = "_test_write_clone"
        CreateSingleValuedWorkspace(OutputWorkspace=wsname)
        values = ["apple", "broccoli", "sourdough", "cheese"]

        ref = WorkspaceMetadata.parse_obj(dict(zip(self.properties, values)))

        # now run the algorithm to write the logs
        algo = WriteWorkspaceMetadata()
        algo.initialize()
        algo.setProperty("Workspace", wsname)
        algo.setProperty("WorkspaceMetadata", ref.json())
        algo.execute()

        # clone the first workspace
        wsname2 = CloneWorkspace(wsname)

        # read the metadata back
        read = ReadWorkspaceMetadata()
        read.initialize()
        read.setProperty("Workspace", wsname2)
        read.execute()

        # verify the metadata object exists and matches original
        ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
        assert ref == ans

        # clone the second workspace
        wsname3 = CloneWorkspace(wsname2)

        # read the metadata back
        read = ReadWorkspaceMetadata()
        read.initialize()
        read.setProperty("Workspace", wsname3)
        read.execute()

        # verify the metadata object exists and matches original
        ans = WorkspaceMetadata.parse_raw(read.getPropertyValue("WorkspaceMetadata"))
        assert ref == ans


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

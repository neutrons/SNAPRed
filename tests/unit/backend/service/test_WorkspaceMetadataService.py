import unittest

import pytest
from mantid.simpleapi import AddSampleLogMultiple, CreateSingleValuedWorkspace, mtd
from pydantic import ValidationError
from util.helpers import deleteWorkspaceNoThrow

from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.meta.Config import Config

thisService = "snapred.backend.service.WorkspaceMetadataService."
TAG_PREFIX = Config["metadata.tagPrefix"]


class TestMetadataService(unittest.TestCase):
    properties = list(WorkspaceMetadata.model_json_schema()["properties"].keys())
    propLogNames = [f"{TAG_PREFIX}{prop}" for prop in properties]

    @classmethod
    def setUpClass(cls):
        cls.instance = WorkspaceMetadataService()
        super().setUpClass()

    def setUp(self):
        self.values = ["exists", "alternate"]
        self.metadata = WorkspaceMetadata.model_validate(dict(zip(self.properties, self.values)))
        return super().setUp()

    def tearDown(self) -> None:
        """
        Delete all workspaces created by previous tests.
        """
        for ws in mtd.getObjectNames():
            deleteWorkspaceNoThrow(ws)
        return super().tearDown()

    def _make_workspace(self, wsname: str = "test_metadata_service") -> str:
        wsname = mtd.unique_name(prefix=wsname)
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

    def test_fail_write_invalid(self):
        wsname = self._make_workspace()
        # first ensure it can be written with a valid dictionary
        assert self.instance.writeWorkspaceMetadata(wsname, {"diffcalState": "exists"})
        # now ensure it fails with an invalid dictionary
        with pytest.raises(ValidationError):
            self.instance.writeWorkspaceMetadata(wsname, {"chips": "exists"})
        with pytest.raises(ValidationError):
            self.instance.writeWorkspaceMetadata(wsname, {"diffcalState": "kiwi"})

    def test_fail_read_invalid(self):
        wsname = self._make_workspace()
        # write invalid logs
        invalidValues = ["newt" for prop in self.propLogNames]
        AddSampleLogMultiple(
            Workspace=wsname,
            LogNames=self.propLogNames,
            LogValues=invalidValues,
        )
        self._verify_logs(wsname, invalidValues)
        with pytest.raises(ValidationError):
            self.instance.readWorkspaceMetadata(wsname)


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

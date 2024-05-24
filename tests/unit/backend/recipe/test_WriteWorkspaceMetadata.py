import unittest
from typing import Dict, Literal
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
from pydantic import BaseModel, ConfigDict, ValidationError

# the algorithm to test
from snapred.backend.dao.WorkspaceMetadata import UNSET
from snapred.backend.recipe.ReadWorkspaceMetadata import ReadWorkspaceMetadata
from snapred.backend.recipe.WriteWorkspaceMetadata import WriteWorkspaceMetadata
from snapred.meta.Config import Config
from util.helpers import deleteWorkspaceNoThrow

thisRecipe = "snapred.backend.recipe.WriteWorkspaceMetadata."
readRecipe = "snapred.backend.recipe.ReadWorkspaceMetadata."
TAG_PREFIX = Config["metadata.tagPrefix"]


# this mocks out the original workspace metadata class
# to ensure the algorithm is as generic as possible in case of later extension
class WorkspaceMetadata(BaseModel):
    fruit: Literal[UNSET, "apple", "pear", "orange"] = UNSET
    veggie: Literal[UNSET, "broccoli", "cauliflower"] = UNSET
    bread: Literal[UNSET, "sourdough", "pumpernickel"] = UNSET
    dairy: Literal[UNSET, "milk", "cheese", "yogurt", "butter"] = UNSET

    model_config = ConfigDict(extra="forbid")


# NOTE do NOT remove these patches
# if your test fails with these patches, then fix your test instead
@mock.patch(thisRecipe + "WorkspaceMetadata", WorkspaceMetadata)
@mock.patch(readRecipe + "WorkspaceMetadata", WorkspaceMetadata)
class TestWriteWorkspaceMetadata(unittest.TestCase):
    properties = list(WorkspaceMetadata.model_json_schema()["properties"].keys())
    propLogNames = [TAG_PREFIX + prop for prop in properties]

    def setUp(self):
        self.basicValues = ["apple", "broccoli", "sourdough", "cheese"]
        self.altValues = ["pear", "cauliflower", "pumpernickel", "milk"]
        self.metadata = WorkspaceMetadata.model_validate(dict(zip(self.properties, self.basicValues)))
        return super().setUp()

    def tearDown(self) -> None:
        """
        Delete all workspaces created by previous tests.
        """
        for ws in mtd.getObjectNames():
            deleteWorkspaceNoThrow(ws)
        return super().tearDown()

    def _make_groceries(self, wsname: str = "test_write_metadata") -> Dict[str, str]:
        wsname = mtd.unique_name(prefix=wsname)
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

    def _verify_logs(self, groceries, values):
        # verify the properties exist and have correct values
        run = mtd[groceries["workspace"]].getRun()
        for value, prop in zip(values, self.propLogNames):
            assert run.hasProperty(prop)
            assert value == run.getLogData(prop).value

    def test_write_metadata(self):
        groceries = self._make_groceries()

        # write the metadata
        assert WriteWorkspaceMetadata().cook(self.metadata, groceries)

        # read the metadata, verify
        ans = ReadWorkspaceMetadata().cook(groceries)
        assert self.metadata == ans
        self._verify_logs(groceries, self.basicValues)

    def test_write_partial(self):
        NUM = len(self.properties)

        for i in range(NUM):
            groceries = self._make_groceries()

            # create a metadata object missing one value
            reducedProperties = [self.properties[j] for j in range(NUM) if j != i]
            reducedValues = [self.basicValues[j] for j in range(NUM) if j != i]
            ref = WorkspaceMetadata.model_validate(dict(zip(reducedProperties, reducedValues)))

            # write the metadata
            assert WriteWorkspaceMetadata().cook(ref, groceries)

            # read the metadata back, verify it is same and has dropped property unset
            ans = ReadWorkspaceMetadata().cook(groceries)
            assert ref == ans
            assert getattr(ans, self.properties[i]) == UNSET
            # verify directly on workspace
            reducedValues.insert(i, UNSET)
            self._verify_logs(groceries, reducedValues)

    def test_write_retain(self):
        """
        If the workspace has a property set, and write is called
        with a metadata object having an unset property,
        then retain the set property from the workspace's logs
        """
        NUM = len(self.properties)

        for i in range(NUM):
            # create a workspace with one log already there
            groceries = self._make_groceries()
            AddSampleLog(
                Workspace=groceries["workspace"],
                LogName=self.propLogNames[i],
                LogText=self.altValues[i],  # NOTE altValues, not basicValues
            )
            # ensure the log was set
            run = mtd[groceries["workspace"]].getRun()
            assert run.hasProperty(self.propLogNames[i])
            assert run.getLogData(self.propLogNames[i]).value == self.altValues[i]

            # create a metadata object missing that property
            reducedProperties = [self.properties[j] for j in range(NUM) if j != i]
            reducedValues = [self.basicValues[j] for j in range(NUM) if j != i]  # NOTE basicValues not altValues
            ref = WorkspaceMetadata.model_validate(dict(zip(reducedProperties, reducedValues)))
            assert getattr(ref, self.properties[i]) == UNSET

            # write the metadata
            assert WriteWorkspaceMetadata().cook(ref, groceries)

            # verify the metadata object exists and has the previously set property is set
            ans = ReadWorkspaceMetadata().cook(groceries)
            for prop in self.properties:  # ALL properties are set on ans
                assert getattr(ans, prop, None) is not None
            for prop in reducedProperties:  # all properties from ref were set to workspace
                assert getattr(ref, prop) == getattr(ans, prop)
            # the property missing form ref has been overwriten in ans
            assert getattr(ref, self.properties[i]) != getattr(ans, self.properties[i])
            assert getattr(ans, self.properties[i]) != UNSET
            # also check directly on the workspace
            reducedValues.insert(i, self.altValues[i])
            self._verify_logs(groceries, reducedValues)

    def test_overwrite_prev_setting(self):
        """
        If a workspace has a property set, and write is called
        with a metadata object with a different property setting,
        then overwrite the set property
        """
        NUM = len(self.properties)

        for i in range(NUM):
            # create a workspace with one log already there
            groceries = self._make_groceries()
            AddSampleLog(
                Workspace=groceries["workspace"],
                LogName=self.propLogNames[i],
                LogText=self.altValues[i],
            )
            run = mtd[groceries["workspace"]].getRun()
            assert run.hasProperty(self.propLogNames[i])
            assert run.getLogData(self.propLogNames[i]).value == self.altValues[i]

            # write the metadata
            assert WriteWorkspaceMetadata().cook(self.metadata, groceries)

            # read the metadata back and verify matches original
            ans = ReadWorkspaceMetadata().cook(groceries)
            assert self.metadata == ans

            # also check directly on the workspace that he value changed
            assert run.hasProperty(self.propLogNames[i])
            assert run.getLogData(self.propLogNames[i]).value == self.basicValues[i]
            assert run.getLogData(self.propLogNames[i]).value != self.altValues[i]

    def test_transform_retain_logs(self):
        """
        Check that the metadata tags are retained when the workspace is transformed
        Here using a simple algorithm, Power, to cube the value "2"
        """
        groceries = self._make_groceries()

        # write the metadata
        assert WriteWorkspaceMetadata().cook(self.metadata, groceries)
        # transform the first workspace
        wstransform = Power(groceries["workspace"], 3)
        assert wstransform.dataY(0) != mtd[groceries["workspace"]].dataY(0)

        # read the metadata back and verify matches original
        ref = ReadWorkspaceMetadata().cook(groceries)
        ans = ReadWorkspaceMetadata().cook({"workspace": wstransform.name()})
        assert ref == ans

    def test_clone_retain_logs(self):
        """
        Check that the metadata tags are persisted through clones
        """
        groceries = self._make_groceries()

        # write the metadata
        assert WriteWorkspaceMetadata().cook(self.metadata, groceries)

        # clone the first workspace
        wsclone = CloneWorkspace(groceries["workspace"])

        # read the metadata back and verify matches original
        ref = ReadWorkspaceMetadata().cook(groceries)
        ans = ReadWorkspaceMetadata().cook({"workspace": wsclone.name()})
        assert ref == ans

        # clone the cloned workspace
        wsclone2 = CloneWorkspace(wsclone)

        # read the metadata back and verify matches original
        ans2 = ReadWorkspaceMetadata().cook({"workspace": wsclone2.name()})
        assert ref == ans2
        assert ans == ans2

    def test_invalid_workspace(self):
        """
        Test algorithm failure if using an invalid workspace name.
        """
        wsname = "_test_write_invalid_double"
        groceries = {"workspace": wsname}
        assert not mtd.doesExist(wsname)

        # now run with the invalid workspace
        with pytest.raises(RuntimeError) as e:
            WriteWorkspaceMetadata().validateInputs(self.metadata, groceries)
        assert "ADS" in str(e)

    def test_invalid_dao(self):
        """
        Test algorithm failure if passing an invalid object as the DAO.
        Note that with mypy this check would be performed in linting, but generally
        the python typehints are ignored when the code runs.
        """
        groceries = self._make_groceries()

        # DAO with invalid setting
        bad_metadata = {"fruit": "turnip"}
        with pytest.raises(ValidationError):
            WorkspaceMetadata.model_validate(bad_metadata)

        # now run with the invalid DAO
        with pytest.raises(ValidationError) as e:
            WriteWorkspaceMetadata().validateInputs(bad_metadata, groceries)
        assert "WorkspaceMetadata" in str(e)

        # DAO with invalid property
        bad_metadata = {"pastry": "turnover"}
        with pytest.raises(ValidationError):
            WorkspaceMetadata.model_validate(bad_metadata)

        # now run the invalid DAO
        with pytest.raises(ValidationError) as e:
            WriteWorkspaceMetadata().validateInputs(bad_metadata, groceries)
        assert "WorkspaceMetadata" in str(e)

    def test_cater(self):
        groceries = self._make_groceries()
        assert WriteWorkspaceMetadata().cater([(self.metadata, groceries)])


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

# ruff: noqa: E402, ARG005

import importlib
import logging
import os
import socket
import tempfile

# Place test-specific imports after other required imports, in order to retain the import order
#   as much as possible.
import unittest
from datetime import datetime
from pathlib import Path
from random import randint
from typing import List
from unittest import mock

import pytest
from util.dao import DAOFactory

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexedObject import IndexedObject
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.data.Indexer import DEFAULT_RECORD_TYPE, Indexer, IndexerType
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

IndexerModule = importlib.import_module(Indexer.__module__)


class TestIndexer(unittest.TestCase):
    ## some helpers for the tests ##

    @classmethod
    def setUpClass(cls):
        cls.instrumentState = DAOFactory.default_instrument_state

    def setUp(self):
        self.tmpDir = tempfile.TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/")
        self.path = Path(self.tmpDir.name)

    def tearDown(self):
        self.tmpDir.cleanup()

    def prepareIndex(self, versions: List[int]):
        index = [self.indexEntry(version) for version in versions]
        write_model_list_pretty(index, self.indexPath())

    def prepareRecords(self, versions: List[int]):
        for version in versions:
            self.writeRecordVersion(version)

    def prepareCalculationParameters(self, versions: List[int]):
        # create calculation parameters for all versions
        # and write them to disk
        for version in versions:
            self.writeCalculationParametersVersion(version)

    def prepareVersions(self, versions: List[int]):
        self.prepareIndex(versions)
        self.prepareRecords(versions)
        self.prepareCalculationParameters(versions)

    def initIndexer(self, indexerType=IndexerType.DEFAULT):
        # create an indexer of specific type inside the temporrary directory
        return Indexer(indexerType=indexerType, directory=self.path)

    def indexEntry(self, version=None):
        # create an index entry with specific version
        # and random other information
        if version is None:
            version = randint(2, 120)
        runNumber = str(randint(1000, 5000))
        return IndexEntry(
            runNumber=runNumber,
            useLiteMode=bool(randint(0, 1)),
            version=version,
            appliesTo=f">={runNumber}",
        )

    def record(self, version=None, *, runNumber=None):
        # create a record with specific version
        # runNumber may be optionally specified
        # otherwise information is random
        if version is None:
            version = randint(2, 120)
        if runNumber is None:
            runNumber = randint(1000, 5000)
        calculationParameters = self.calculationParameters(version)
        indexEntry = self.indexEntry(version)
        indexEntry.appliesTo = f">={runNumber}"
        return Record(
            runNumber=runNumber,
            useLiteMode=bool(randint(0, 1)),
            version=version,
            calculationParameters=calculationParameters,
            indexEntry=indexEntry,
        )

    def calculationParameters(self, version):
        # create state parameters with a specific version
        return CalculationParameters(
            instrumentState=self.instrumentState,
            seedRun=randint(1000, 5000),
            useLiteMode=bool(randint(0, 1)),
            creationDate=datetime.today().isoformat(),
            name="",
            version=version,
            indexEntry=DAOFactory.indexEntryBoilerplate,
        )

    def indexEntryFromRecord(self, record: Record) -> IndexEntry:
        return IndexEntry(
            runNumber=record.runNumber,
            useLiteMode=record.useLiteMode,
            version=record.version,
            appliesTo=f">={record.runNumber}",
            author="",
            comments="",
            timestmp=0,
        )

    def recordFromIndexEntry(self, entry: IndexError) -> Record:
        # given an index entry, create a bare record corresponding
        return Record(
            runNumber=entry.runNumber,
            useLiteMode=entry.useLiteMode,
            version=entry.version,
            calculationParameters=self.calculationParameters(entry.version),
            indexEntry=entry,
        )

    def indexPath(self):
        # the path where indices should be written
        return self.path / "Index.json"

    def versionPath(self, version):
        # a path where version files should be written
        return self.path / wnvf.pathVersion(version)

    def recordPath(self, version):
        # a filepath where records should be written
        return self.versionPath(version) / "Record.json"

    def parametersPath(self, version):
        # a filepath where records should be written
        return self.versionPath(version) / "CalculationParameters.json"

    def makeVersionDir(self, version):
        self.versionPath(version).mkdir(exist_ok=True)

    def writeIndex(self, index: List[IndexEntry]):
        write_model_list_pretty(index, self.indexPath())

    def writeRecord(self, record: Record):
        # write a record independently of the indexer
        # used to verify loading of previous records
        self.makeVersionDir(record.version)
        write_model_pretty(record, self.recordPath(record.version))
        write_model_pretty(record.calculationParameters, self.parametersPath(record.version))

    def writeRecordVersion(self, version, *, runNumber=None):
        # create and write a record with a specific version and optional run number
        record = self.record(version, runNumber=runNumber)
        self.writeRecord(record)

    def writeCalculationParametersVersion(self, version):
        # create and write calculation parameters with a specific version
        params = self.calculationParameters(version)
        write_model_pretty(params, self.parametersPath(version))

    ## TESTS OF INITIALIZER ##

    def test_init_nothing(self):
        # when initialized, the index is bare
        indexer = self.initIndexer()
        assert indexer.index == {}
        assert indexer.currentVersion() is None

    def test_init_versions_exist(self):
        # when initialized, existing information is loaded
        versionList = [1, 2, 3, 4]
        self.prepareVersions(versionList)

        indexer = self.initIndexer()

        assert list(indexer.index.keys()) == versionList
        assert indexer.currentVersion() == max(versionList)

    def test_init_versions_missing_index(self):
        # create a situation where the index is missing a value shown in the directory tree
        # then the indexer should create an index entry based off of the directory record
        missingVersion = 3
        recordVersions = [1, 2, 3, 4]
        indexVersions = [v for v in recordVersions if v != missingVersion]

        self.prepareIndex(indexVersions)
        self.prepareRecords(recordVersions)

        with self.assertLogs(logger=IndexerModule.logger, level=logging.WARNING) as cm:
            indexer = self.initIndexer()
        assert "Another user may be calibrating/updating the same directory." in cm.output[0]

        assert list(indexer.index.keys()) == recordVersions

    def test_init_versions_missing_directory(self):
        # create a situation where the index has a value not reflected in directory tree
        # then the indexer should raise an error
        missingVersion = 3
        indexVersions = [1, 2, 3, 4]
        recordVersions = [v for v in indexVersions if v != missingVersion]
        self.prepareIndex(indexVersions)
        self.prepareRecords(recordVersions)

        with (  # noqa: PT012
            pytest.raises(FileNotFoundError) as e,
            mock.patch.dict(IndexerModule.sys.modules) as mockModules,
        ):
            # "pytest" in `sys.modules` will suppress the exception,
            #     which otherwise always occurs during the teardown,
            #       even when nothing is wrong.
            del mockModules["pytest"]
            self.initIndexer()
        assert str(missingVersion) in str(e)

    def x_test_delete_save_on_exit(self):
        # ensure the index list is saved when the indexer is deleted
        versionList = [1, 2, 3]
        self.prepareVersions(versionList)
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)
        assert not self.indexPath().exists()

        indexer = self.initIndexer()
        del indexer
        assert self.indexPath().exists()
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

    def x_test_delete_reconcile_versions(self):
        # ensure the versions in the index are reconciled to file tree during save-on-delete
        versionList = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)

        # add an index entry but don't save a record
        # the saved index will not have the next version
        indexer = self.initIndexer()
        indexer.addIndexEntry(self.indexEntry(4))
        del indexer
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

        # add a record but not an index
        # the index will have a version created from the record
        indexer = self.initIndexer()
        self.writeRecordVersion(4)
        del indexer
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList) + 1

    ### TEST VERSION GETTER METHODS ###

    def test_allVersions_empty(self):
        indexer = self.initIndexer()
        assert indexer.allVersions() == []

    def test_allVersions_some(self):
        versions = [1, 2, 3, 4, 5]
        index = {version: self.indexEntry(version) for version in versions}
        indexer = self.initIndexer()
        indexer.index = index
        assert indexer.allVersions() == versions

    def test_defaultVersion(self):
        indexer = self.initIndexer()
        assert indexer.defaultVersion() == VERSION_START()

    def test_currentVersion_none(self):
        # ensure the current version of an empty index is unitialized
        indexer = self.initIndexer()
        assert indexer.currentVersion() is None
        # if there is no current version then there is no current path on disk.
        with pytest.raises(ValueError, match=r".*The indexer has encountered an invalid version*"):
            indexer.currentPath() == self.versionPath(VERSION_START())

    def test_flattenVersion(self):
        indexer = self.initIndexer()
        indexer.currentVersion = lambda: 3
        indexer.nextVersion = lambda: 4
        assert indexer._flattenVersion(VersionState.DEFAULT) == indexer.defaultVersion()
        assert indexer._flattenVersion(VersionState.NEXT) == indexer.nextVersion()
        assert indexer._flattenVersion(3) == 3

        with pytest.raises(ValueError, match=r".*Version must be an int or*"):
            indexer._flattenVersion(None)

    def test_writeNewVersion_noAppliesTo(self):
        # ensure that a new record is written to disk
        # and the index is updated to reflect the new record
        indexer = self.initIndexer()
        version = randint(2, 120)
        record = self.record(version)
        entry = record.indexEntry
        entry.appliesTo = None
        indexer.writeRecord(record)
        assert self.recordPath(version).exists()
        assert indexer.index[version] == entry

    def test_writeNewVersion_recordAlreadyExists(self):
        # ensure that a new record is written to disk
        # and the index is updated to reflect the new record
        indexer = self.initIndexer()
        version = randint(2, 120)
        record = self.record(version)
        entry = record.indexEntry
        indexer.writeRecord(record)
        assert self.recordPath(version).exists()
        assert indexer.index[version] == entry

        # now write the record again
        # ensure that the record is overwritten
        record = self.record(version)
        entry = record.indexEntry

        with pytest.raises(ValueError, match=".*already exists.*"):
            indexer.writeRecord(record)

    def test_currentVersion_add(self):
        # ensure current version advances when index entries are written
        # prepare directories for the versions
        versions = [2, 3, 4]
        index = {version: self.indexEntry(version) for version in versions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versions:
            self.writeRecordVersion(version)

        indexer = self.initIndexer()
        # ensure we are in position we expect:
        assert list(indexer.index.keys()) == versions
        assert list(indexer.readDirectoryList()) == versions
        # now check that the current version advances when an entry is added
        assert indexer.currentVersion() == max(versions)
        indexer.addIndexEntry(self.indexEntry(indexer.nextVersion()))
        assert indexer.currentVersion() == max(versions) + 1

    def test_currentVersion_dirHigher(self):
        # if there is a directory not represented in the index: warn and ignore
        missingVersion = 3
        recordVersions = [1, 2, 3]
        indexVersions = [v for v in recordVersions if v != missingVersion]
        self.prepareRecords(recordVersions)
        self.prepareIndex(indexVersions)

        with self.assertLogs(logger=IndexerModule.logger, level=logging.WARNING) as cm:
            indexer = self.initIndexer()
        assert "Another user may be calibrating/updating the same directory." in cm.output[0]
        assert indexer.currentVersion() == len(recordVersions)

    def test_currentVersion_indexhigher(self):
        # if there is an index entry not represented in the directory: throw error
        dirVersions = [1, 2]
        indexVersions = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in indexVersions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in dirVersions:
            self.writeRecordVersion(version)

        with (  # noqa: PT012
            pytest.raises(FileNotFoundError) as e,
            mock.patch.dict(IndexerModule.sys.modules) as mockModules,
        ):
            # "pytest" in `sys.modules` will suppress the exception,
            #     which otherwise always occurs during the teardown,
            #       even when nothing is wrong.
            del mockModules["pytest"]
            self.initIndexer()
        assert str(3) in str(e)

    def test_latestApplicableVersion_none(self):
        # ensure latest applicable version is none if no versions apply
        runNumber = "123"
        versionList = [3, 4, 5]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # there is no applicable version
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest is None

    def test_latestApplicableVersion_one(self):
        # ensure latest applicable versiom is the one if one applies
        runNumber = "123"
        versionList = [3, 4, 5]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # make one entry applicable
        version = 4
        indexer.index[version].appliesTo = f">={runNumber}"
        # get latest applicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert version == latest

    def test_latestApplicableVersion_some(self):
        # ensure latest applicable version will sort several applicable versions
        runNumber = "123"
        versionList = [3, 4, 5, 6, 7]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # make some entries applicable
        applicableVersions = [4, 6]
        for version in applicableVersions:
            indexer.index[version].appliesTo = f">={runNumber}"
        # get latest applicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == applicableVersions[-1]

    def test_latestApplicableVersion_sorts_in_time(self):
        # ensure latest applicable version will sort several applicable versions
        runNumber = "123"
        versionList = [3, 4, 5, 6, 7]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # make some entries applicable and set their timestamps in reverse
        applicableVersions = [6, 4]
        for i, version in enumerate(applicableVersions):
            indexer.index[version].appliesTo = f">={runNumber}"
            indexer.index[version].timestamp = i + 1
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == applicableVersions[-1]
        assert latest != max(applicableVersions)

    def test_latestApplicableVersion_returns_default(self):
        # ensure latest applicable version will be default if it is the only one
        runNumber = "123"
        versionList = [VERSION_START()]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # make it applicable
        indexer.index[indexer.defaultVersion()].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == VERSION_START()

    def test_latestApplicableVersion_excludes_default(self):
        # ensure latest applicable version will remove default if other runs exist
        runNumber = "123"
        versionList = [VERSION_START(), 4, 5]
        self.prepareVersions(versionList)
        indexer = self.initIndexer()
        # make some entries applicable
        applicableVersions = [VERSION_START(), 4]
        print(indexer.index)
        for version in applicableVersions:
            indexer.index[version].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == applicableVersions[-1]

    def test_getLatestApplicableVersion(self):
        # make one applicable entry
        version1 = randint(1, 10)
        entry1 = self.indexEntry(version1)
        entry1.appliesTo = "123"
        # make a non-applicable entry
        version2 = randint(11, 20)
        entry2 = self.indexEntry(version2)
        entry2.appliesTo = ">123"
        # add both entries to index
        indexer = self.initIndexer()
        indexer.index = {version1: entry1, version2: entry2}
        # only the applicable entry is returned
        assert indexer.latestApplicableVersion("123") == version1

    def test_isValidVersion(self):
        indexer = self.initIndexer()
        # the good
        for i in range(10):
            assert indexer.validateVersion(randint(2, 120))
        assert indexer.validateVersion(VersionState.DEFAULT)
        # the bad
        badInput = ["bad", "*", None, -2, 1.2]
        for i in badInput:
            with pytest.raises(ValueError, match=r".*The indexer has encountered an invalid version*"):  # noqa: PT012
                indexer.validateVersion(i)
                pytest.fail(f"Expected ValueError for input {i}, but it was not raised.")  # noqa: PT012

    def test_nextVersion(self):
        # check that the current version advances as expected as
        # both index entries and records are added to the index
        # NOTE all double-calls are deliberate to ensure no change in state on call

        expectedIndex = {}
        indexer = self.initIndexer()
        assert indexer.index == expectedIndex
        # there is no current version
        assert indexer.currentVersion() is None
        # the first "next" version is the start
        assert indexer.nextVersion() == VERSION_START()

        # add an entry to the calibration index
        here = VERSION_START()
        # it should be added at the start
        entry = self.indexEntry(indexer.nextVersion())
        indexer.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexer.index == expectedIndex
        # the current version should be this version
        assert indexer.currentVersion() == here
        # the next version also should be this version
        # until a record is written to disk
        assert indexer.nextVersion() == here
        expectedIndex.pop(here)
        assert indexer.index == expectedIndex

        # NOTE: At this point, the index will have been purged because
        #       the entry did not have a corresponding record on disk.
        # PREVIOUSLY: This had been testing against a ghost entry which
        #             did not represent a record on disk,
        #             but was still in the index.
        #             This is no longer the case.

        # now write the record
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> WRITE 1 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        record = self.recordFromIndexEntry(entry)
        indexer.writeRecord(record)
        expectedIndex[here] = entry

        # the current version hasn't moved
        assert indexer.currentVersion() == here
        # the next version will be one past this one
        assert indexer.nextVersion() == here + 1
        # ensure no change
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> WRITE 2 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        # write a record first, at a much future version
        # then add an index entry, and ensure it matches
        here = here + 23
        record = self.record(here)
        entry = record.indexEntry
        expectedIndex[entry.version] = entry
        indexer.writeRecord(record)
        assert indexer.nextVersion() == here + 1
        assert indexer.nextVersion() not in indexer.index

        # now add the entry
        entry = self.indexEntryFromRecord(record)
        indexer.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexer.index == expectedIndex
        # enssure match
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1

    def test_nextVersion_with_default_index_first(self):
        # check that indexer handles the default version as expected

        expectedIndex = {}
        indexer = self.initIndexer()
        assert indexer.index == expectedIndex

        # there is no current version
        assert indexer.currentVersion() is None
        # the first "next" version is the start
        assert indexer.nextVersion() == VERSION_START()

        # add an entry at the default version
        entry = self.indexEntry(VersionState.DEFAULT)
        record = self.recordFromIndexEntry(entry)
        expectedIndex[VERSION_START()] = entry
        indexer.writeRecord(record)
        assert self.recordPath(indexer.defaultVersion()).exists()
        # the current version is still the default version
        assert indexer.currentVersion() == indexer.defaultVersion()
        # the next version will be the starting version + 1
        assert indexer.nextVersion() == VERSION_START() + 1

        # add another entry -- now at the start
        entry = self.indexEntry(indexer.nextVersion())
        expectedIndex[indexer.nextVersion()] = entry
        indexer.addIndexEntry(entry)
        assert indexer.index == expectedIndex
        assert indexer.currentVersion() == VERSION_START() + 1

        # the next version should be the starting version
        # until a record is written
        assert indexer.nextVersion() == VERSION_START() + 1
        expectedIndex.pop(entry.version)
        assert indexer.index == expectedIndex
        # now write the record -- ensure it is written at the
        record = self.recordFromIndexEntry(entry)
        entry = record.indexEntry
        indexer.writeRecord(record)
        assert self.recordPath(VERSION_START()).exists()
        # ensure current still here
        assert indexer.currentVersion() == VERSION_START() + 1
        # ensure next is after here
        assert indexer.nextVersion() == VERSION_START() + 2

    def test_nextVersion_with_default_record_first(self):
        # check default behaves correctly if a record is written first

        indexer = self.initIndexer()
        assert indexer.index == {}

        # there is no current version
        assert indexer.currentVersion() is None

        # the first "next" version is the start
        assert indexer.nextVersion() == VERSION_START()

        # add a record at the default version
        record = self.record(VersionState.DEFAULT)
        assert indexer._flattenVersion(record.version) == VERSION_START()
        indexer.writeRecord(record)

        # the current version is still the default version
        assert indexer.currentVersion() == VERSION_START()
        # the next version will be one past the starting version
        assert indexer.nextVersion() == VERSION_START() + 1

    ### TESTS OF VERSION COMPARISON METHODS ###

    def test__isApplicableEntry_equals(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "123"
        assert indexer._isApplicableEntry(entry, "123")

    def test__isApplicableEntry_greaterThan(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">123"
        assert indexer._isApplicableEntry(entry, "456")

    def test__isApplicableEntry_lessThan(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<123"
        assert indexer._isApplicableEntry(entry, "99")

    def test_isApplicableEntry_lessThanEquals(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<=123"
        assert indexer._isApplicableEntry(entry, "123")
        assert indexer._isApplicableEntry(entry, "99")
        assert not indexer._isApplicableEntry(entry, "456")

    def test_isApplicableEntry_greaterThanEquals(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">=123"
        assert indexer._isApplicableEntry(entry, "123")
        assert indexer._isApplicableEntry(entry, "456")
        assert not indexer._isApplicableEntry(entry, "99")

    ### TESTS OF PATH METHODS ###

    def test_indexPath(self):
        indexer = self.initIndexer()
        assert indexer.indexPath() == self.indexPath()

    def test_recordPath(self):
        indexer = self.initIndexer()
        assert indexer.recordPath(12) == self.recordPath(12)

    def test_versionPath(self):
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()

        with pytest.raises(ValueError, match=".*The indexer has encountered an invalid version*"):
            indexer.versionPath(None)

        # if version is specified, return that one
        for i in versionList:
            ans3 = indexer.versionPath(i)
            assert ans3 == self.versionPath(i)

    def test_currentPath(self):
        # ensure the current path corresponds to the max in the list of versions
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        self.prepareIndex(versionList)
        indexer = self.initIndexer()
        indexer.currentPath() == self.versionPath(max(versionList))

    def test_latestApplicablePath(self):
        # ensure latest applicable path corresponds to correct version
        runNumber = "123"
        versionList = [3, 4, 5]
        index = [self.indexEntry(version) for version in versionList]
        write_model_list_pretty(index, self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make one entry applicable
        version = 4
        indexer.index[version].appliesTo = f">={runNumber}"
        print(indexer.index)
        latest = indexer.latestApplicableVersion(runNumber)
        assert indexer.getLatestApplicablePath(runNumber) == self.versionPath(latest)

    ### TEST INDEX MANIPULATION METHODS ###

    def test_readIndex(self):
        # test that a previously written index is correctly read
        versionList = [randint(0, 120) for i in range(20)]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        self.prepareRecords(versionList)
        indexer = self.initIndexer()
        assert indexer.readIndex() == index

    def test_readIndex_nothing(self):
        indexer = self.initIndexer()
        with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing."):
            indexer.readIndex()

    def test_readWriteIndex(self):
        # test that an index can be read/written correctly
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        indexer = self.initIndexer()
        indexer.index = index
        indexer.writeIndex()
        assert indexer.indexPath().exists()
        ans = indexer.readIndex()
        assert ans == index

    def test_addEntry_to_nothing(self):
        # adding an index entry to an empty index works
        indexer = self.initIndexer()
        assert indexer.index == {}
        assert indexer.currentVersion() is None
        indexer.addIndexEntry(self.indexEntry(3))
        assert indexer.currentVersion() is not None
        # add one more time to make sure no conflicts with things not existing
        indexer.addIndexEntry(self.indexEntry(4))

    def test_addEntry_writes(self):
        # adding an index entry also writes the index entry to disk
        indexer = self.initIndexer()
        for i in range(3, 10):
            indexer.addIndexEntry(self.indexEntry(i))
            readIndex = parse_file_as(List[IndexEntry], indexer.indexPath())
            assert readIndex == list(indexer.index.values())

    def test_addEntry_default(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(indexer.defaultVersion())
        indexer.addIndexEntry(entry)
        assert indexer.defaultVersion() in indexer.index

    def test_addEntry_advances(self):
        # adding an index entry advances the current version
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        assert indexer.currentVersion() == max(versionList)
        indexer.addIndexEntry(self.indexEntry(indexer.nextVersion()))
        assert indexer.currentVersion() == max(versionList) + 1

    def test_addEntry_at_version_new(self):
        # an index entry can be added at any version number, not only next
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        assert 3 not in indexer.index
        indexer.addIndexEntry(self.indexEntry(3))
        assert indexer.index[3] is not None

    def test_addEntry_at_version_overwrite(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        entry7 = indexer.index[7]
        indexer.addIndexEntry(self.indexEntry(7))
        assert indexer.index[7] is not entry7

    ### TEST RECORD READ / WRITE METHODS ###

    # read / write #

    def test_readWriteRecord_next_version(self):
        # make sure there exists another version
        # so that we can know it does not default
        # to the starting version
        versions = [2, 3, 4]
        self.prepareVersions(versions)
        indexer = self.initIndexer()
        nextVersion = indexer.nextVersion()
        assert nextVersion != VERSION_START()
        # now write then read the record
        # make sure the record was saved at the next version
        # and the read / written records match
        record = self.record(nextVersion)
        indexer.writeRecord(record)
        res = indexer.readRecord(nextVersion)
        assert record.version == nextVersion
        assert res == record

    def test_readWriteRecord_any_version(self):
        # write a record at some version number
        version = randint(10, 20)
        record = self.record(version)
        indexer = self.initIndexer()
        # write then read the record
        # make sure the record version was updated
        # and the read / written records match
        indexer.writeRecord(record)
        res = indexer.readRecord(version)
        assert record.version == version
        assert res == record

    # read #

    def test_readRecord_none(self):
        version = randint(1, 11)
        indexer = self.initIndexer()
        indexer.readIndex = mock.Mock()
        assert not self.recordPath(version).exists()
        with pytest.raises(FileNotFoundError, match=r".*No Record found at*"):
            indexer.readRecord(version)

    def test_readRecord(self):
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexer = self.initIndexer()
        indexer.readIndex = mock.Mock()
        res = indexer.readRecord(record.version)
        assert res == record

    def test_readRecord_invalid_version(self):
        # if an invalid version is attempted to be read, raise an error
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexer = self.initIndexer()
        indexer.readIndex = mock.Mock()
        with pytest.raises(ValueError, match=r".*The indexer has encountered an invalid version*"):
            indexer.readRecord("*")

    # write #

    def test_obtainLock(self):
        # ensure the indexer can obtain a lock
        indexer = self.initIndexer()
        lock = indexer.obtainLock()
        assert lock is not None
        lock.release()

    def validateLockfile(self, lock):
        # ensure the lockfile is valid
        assert lock is not None
        assert lock.lockFilePath.exists()
        assert str(lock.lockFilePath).endswith(".lock")
        assert str(lock.lockFilePath).startswith(Config["lockfile.root"])
        assert str(os.getpid()) in str(lock.lockFilePath)
        assert socket.gethostname().split(".")[0] in str(lock.lockFilePath)

    def test_lockContext(self):
        # ensure the indexer can use a context manager to obtain a lock
        indexer = self.initIndexer()
        with indexer._lockContext() as lock:
            self.validateLockfile(lock)
            lockfileContents = lock.lockFilePath.read_text()
            assert str(indexer.rootDirectory) in lockfileContents

    def test_writeRecord_with_version(self):
        # this test ensures a record can be written to the indicated version
        # create a record and write it
        version = randint(2, 120)
        record = self.record(version)
        indexer = self.initIndexer()
        indexer.writeRecord(record)
        assert record.version == version
        assert self.recordPath(version).exists()
        # read it back in and ensure it is the same
        res = parse_file_as(Record, self.recordPath(version))
        assert res == record
        # ensure the version numbers were set
        assert res.version == version
        assert res.calculationParameters.version == version

    def test_writeRecord_next_version(self):
        # this test we can save to next version

        # make sure there exists other versions so that we can know it does not default to the starting version
        versions = [2, 3, 4]
        self.prepareVersions(versions)
        indexer = self.initIndexer()
        nextVersion = indexer.nextVersion()
        assert nextVersion != VERSION_START()
        # now write the record
        record = self.record(nextVersion)
        indexer.writeRecord(record)
        assert record.version == nextVersion
        assert self.recordPath(nextVersion).exists()
        res = parse_file_as(Record, self.recordPath(nextVersion))
        assert res == record
        # ensure the version numbers were set
        assert res.version == nextVersion
        assert res.calculationParameters.version == nextVersion

    # make sure the indexer can read/write specific record types #

    def test_readWriteRecord_calibration(self):
        # prepare the record
        record = DAOFactory.calibrationRecord()
        record.version = randint(2, 100)
        # write then read in the record
        indexer = self.initIndexer(IndexerType.CALIBRATION)
        indexer.writeRecord(record)
        res = indexer.readRecord(record.version)
        assert type(res) is CalibrationRecord
        assert res == record

    def test_readWriteRecord_normalization(self):
        # prepare the record
        record = DAOFactory.normalizationRecord()
        record.version = randint(2, 100)
        # write then read in the record
        indexer = self.initIndexer(IndexerType.NORMALIZATION)
        indexer.writeRecord(record)
        res = indexer.readRecord(record.version)
        assert type(res) is NormalizationRecord
        assert res == record

    ### TEST STATE PARAMETER READ / WRITE METHODS ###

    def test_readIndexedObject_none(self):
        version = randint(1, 100)
        indexer = self.initIndexer()
        indexer.readIndex = mock.Mock()
        assert not self.parametersPath(version).exists()
        with pytest.raises(FileNotFoundError, match=r".*No CalculationParameters found at*"):
            indexer.readParameters(version)

    def test_readWriteIndexedObject(self):
        version = VersionState.NEXT
        indexer = self.initIndexer()
        versionedObj = IndexedObject(version=version, indexEntry=self.indexEntry(version))
        indexer.writeIndexedObject(versionedObj)
        assert indexer.indexedObjectFilePath(IndexedObject, 0).exists()
        res = indexer.readIndexedObject(IndexedObject, 0)
        assert res == versionedObj
        assert res.version == 0

        with pytest.raises(ValueError, match=r".*already exists. \nA version collision has occurred*"):
            indexer.writeIndexedObject(versionedObj)

    def test_readWriteIndexedObject_next_overwrite(self):
        version = VersionState.NEXT
        indexer = self.initIndexer()
        versionedObj = IndexedObject(version=version, indexEntry=self.indexEntry(version))
        indexer.writeIndexedObject(versionedObj)
        assert indexer.indexedObjectFilePath(IndexedObject, 0).exists()
        res = indexer.readIndexedObject(IndexedObject, 0)
        assert res == versionedObj
        assert res.version == 0

        versionedObj = IndexedObject(version=version, indexEntry=self.indexEntry(version))
        indexer.writeIndexedObject(versionedObj)
        res = indexer.readIndexedObject(IndexedObject, 1)
        assert res == versionedObj
        assert res.version == 1

        # Overwrite the versioned object
        versionedObj = IndexedObject(version=1, indexEntry=self.indexEntry(1))
        indexer.writeIndexedObject(versionedObj, overwrite=True)
        latestVersion = indexer.latestApplicableVersion(9999)
        assert latestVersion == 1

    def test__determineRecordType(self):
        indexer = self.initIndexer(IndexerType.CALIBRATION)
        assert indexer._determineRecordType(indexer.defaultVersion()) == DEFAULT_RECORD_TYPE.get(
            IndexerType.CALIBRATION
        )

    def test_readIndex_noIndex(self):
        # ensure that if the index is not present, an error is raised
        indexer = self.initIndexer()
        with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing."):
            indexer.readIndex()

    def test_readIndex_emptyIndex(self):
        indexer = self.initIndexer()
        indexPath = indexer.indexPath()
        if indexPath.exists():
            indexPath.unlink()
        indexPath.write_text("[]")  # write an empty index
        assert indexer.readIndex() == {}

    def test_recoverIndex(self):
        indexer = self.initIndexer()
        indexPath = indexer.indexPath()
        if indexPath.exists():
            indexPath.unlink()
        # write a corrupted index
        indexPath.write_text("corrupted data")
        with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing."):
            indexer.readIndex()
        # recover the index
        self.prepareVersions([1, 2, 3])
        indexer.indexPath().unlink()
        indexer.recoveryMode = True
        indexer.recoverIndex(dryrun=False)
        assert len(indexer.readIndex()) == 3

    def test_recoverIndex_corruptVersions(self):
        indexer = self.initIndexer()
        indexPath = indexer.indexPath()
        if indexPath.exists():
            indexPath.unlink()
        # write a corrupted index
        indexPath.write_text("corrupted data")
        with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing."):
            indexer.readIndex()
        # recover the index
        self.prepareVersions([1, 2, 3, 4, 5])

        # simulate a corrupted folder by removing some records and parameters
        indexer.recordPath(1).unlink()
        indexer.parametersPath(2).unlink()

        # simulate a record with incorrect version
        record = indexer.readRecord(4)
        record.version = 5
        indexer.recordPath(4).write_text(record.model_dump_json())

        # simulate a parameters file with incorrect version
        parameters = indexer.readParameters(5)
        parameters.version = 6
        indexer.parametersPath(5).write_text(parameters.model_dump_json())

        # remove the index
        indexer.indexPath().unlink()

        # recover the index
        # we expect only the valid records and parameters to be recovered
        indexer.recoveryMode = True
        indexer.recoverIndex(dryrun=False)
        assert len(indexer.readIndex()) == 1

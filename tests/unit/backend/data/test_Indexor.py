# ruff: noqa: E402, ARG005

import importlib
import logging
import tempfile
import unittest
from pathlib import Path
from random import randint
from typing import List

import pytest
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VERSION_NONE_NAME, VERSION_START
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

IndexerModule = importlib.import_module(Indexer.__module__)


class TestIndexer(unittest.TestCase):
    ## some helpers for the tests ##

    @classmethod
    def setUpClass(cls):
        calibration = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        cls.instrumentState = calibration.instrumentState

    def setUp(self):
        self.tmpDir = tempfile.TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/")
        self.path = Path(self.tmpDir.name)

    def tearDown(self):
        self.tmpDir.cleanup()

    def initIndexer(self, indexerType=IndexerType.DEFAULT):
        # create an indexer of specific type inside the temporrary directory
        return Indexer(indexerType=indexerType, directory=self.path)

    def indexEntry(self, version=None):
        # create an index entry with specific version
        # and random other information
        if version is None:
            version = randint(2, 120)
        return IndexEntry(
            runNumber=str(randint(1000, 5000)),
            useLiteMode=bool(randint(0, 1)),
            version=version,
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
        return Record(
            runNumber=runNumber,
            useLiteMode=bool(randint(0, 1)),
            version=version,
            calculationParameters=calculationParameters,
        )

    def calculationParameters(self, version):
        # create state parameters with a specific version
        return CalculationParameters(
            instrumentState=self.instrumentState,
            seedRun=randint(1000, 5000),
            useLiteMode=bool(randint(0, 1)),
            creationDate=0,
            name="",
            version=version,
        )

    def recordFromIndexEntry(self, entry: IndexError) -> Record:
        # given an index entry, create a bare record corresponding
        return Record(
            runNumber=entry.runNumber,
            useLiteMode=entry.useLiteMode,
            version=entry.version,
            calculationParameters=self.calculationParameters(entry.version),
        )

    def indexPath(self):
        # the path where indices should be written
        return self.path / "Index.json"

    def versionPath(self, version):
        # a path where version files should be written
        return self.path / wnvf.fileVersion(version)

    def recordPath(self, version):
        # a filepath where records should be written
        return self.versionPath(version) / "Record.json"

    def parametersPath(self, version):
        # a filepath where records should be written
        return self.versionPath(version) / "Parameters.json"

    def makeVersionDir(self, version):
        self.versionPath(version).mkdir()

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

    ## TESTS OF INITIALIZER ##

    def test_init_nothing(self):
        # when initialized, the index is bare
        indexer = self.initIndexer()
        assert indexer.index == {}
        assert indexer.currentVersion() is None

    def test_init_versions_exist(self):
        # when initialized, existing information is loaded
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.makeVersionDir(version)

        indexer = self.initIndexer()

        assert indexer.index == index
        assert indexer.currentVersion() == max(versionList)

    def test_init_versions_missing_index(self):
        # create a situation where the index is missing a value shown in the directory tree
        # then the indexer should create an index entry based off of the directory record
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        # remove version 3 and write the index
        del index[3]
        write_model_list_pretty(list(index.values()), self.indexPath())
        # now add all the needed files
        # - add a v_000x/ directory for each version
        # - add a v_000x/CalibrationRecord.json for each version
        for version in versionList:
            self.writeRecordVersion(version)

        indexer = self.initIndexer()

        # what we expect -- an index with version 1, 2, 3, 4
        # version 3 will be created from the record
        # the timestamps will necessarily differ, so set them both to zero
        expectedIndex = index.copy()
        expectedIndex[3] = indexer.indexEntryFromRecord(indexer.readRecord(3))
        expectedIndex[3].timestamp = 0
        indexer.index[3].timestamp = 0

        assert indexer.index == expectedIndex

    def test_init_versions_missing_directory(self):
        # create a situation where the index has a value not reflected in directory tree
        # then the indexer should delete that index entry
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        # remove version 3
        versionList.remove(3)
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)

        indexer = self.initIndexer()

        # what we expect -- an index with version 1, 2, 4
        # version 3 will have been deleted
        expectedIndex = index.copy()
        del expectedIndex[3]

        assert indexer.index == expectedIndex

    def test_delete_save_on_exit(self):
        # ensure the index list is saved when the indexer is deleted
        versionList = [1, 2, 3]
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)
        assert not self.indexPath().exists()

        indexer = self.initIndexer()
        del indexer
        assert self.indexPath().exists()
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

    def test_delete_reconcile_versions(self):
        # ensure the version in the idex are reconciled to file tree during save-on-delete
        versionList = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)

        # add an index entry but don't save a file
        # the index will not have the next version
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
        assert indexer.defaultVersion() == VERSION_DEFAULT

    def test_currentVersion_none(self):
        # ensure the current version of an empty index is unitialized
        indexer = self.initIndexer()
        assert indexer.currentVersion() is None
        # the path should go to the starting version
        indexer.currentPath() == self.versionPath(VERSION_START)

    def test_currentVersion_add(self):
        # ensure current version advances when index entries are written
        # prepare directories for the versions
        versions = [2, 3, 4]
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
        # ensure the current index is set to the max in the directory tree
        # when the index and directory disagree
        # in this case, the directory tree is higher than the index
        dirVersions = [1, 2, 3]
        indexVersions = [1, 2]
        index = {version: self.indexEntry(version) for version in indexVersions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in dirVersions:
            self.writeRecordVersion(version)

        indexer = self.initIndexer()
        assert indexer.currentVersion() == max(dirVersions)

    def test_currentVersion_indexhigher(self):
        # ensure the current index is set to the max in the directory tree
        # when the index and directory disagree
        # in this case, the index is higher than the directory tree
        dirVersions = [1, 2]
        indexVersions = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in indexVersions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in dirVersions:
            self.writeRecordVersion(version)

        indexer = self.initIndexer()
        assert indexer.currentVersion() == max(dirVersions)

    def test_latestApplicableVersion_none(self):
        # ensure latest applicable version is none if no versions apply
        runNumber = "123"
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # there is no applivcable version
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest is None

    def test_latestApplicableVersion_one(self):
        # ensure latest applicable versiom is the one if one applies
        runNumber = "123"
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make one entry applicable
        version = 4
        indexer.index[version].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert version == latest

    def test_latestApplicableVersion_some(self):
        # ensure latest applicable version will sort several applicable versions
        runNumber = "123"
        versionList = [3, 4, 5, 6, 7]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make some entries applicable
        applicableVersions = [4, 6]
        for version in applicableVersions:
            indexer.index[version].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == applicableVersions[-1]

    def test_latestApplicableVersion_sorts_in_time(self):
        # ensure latest applicable version will sort several applicable versions
        runNumber = "123"
        versionList = [3, 4, 5, 6, 7]
        for version in versionList:
            self.writeRecordVersion(version)
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
        versionList = [VERSION_DEFAULT]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make it applicable
        indexer.index[VERSION_DEFAULT].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == VERSION_DEFAULT

    def test_latestApplicableVersion_excludes_default(self):
        # ensure latest applicable version will remove default if other runs exist
        runNumber = "123"
        versionList = [VERSION_DEFAULT, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make some entries applicable
        applicableVersions = [VERSION_DEFAULT, 4]
        print(indexer.index)
        for version in applicableVersions:
            indexer.index[version].appliesTo = f">={runNumber}"
        # get latest apllicable
        latest = indexer.latestApplicableVersion(runNumber)
        assert latest == applicableVersions[-1]

    def test_thisOrCurrentVersion(self):
        version = randint(20, 120)
        indexer = self.initIndexer()
        assert indexer.thisOrCurrentVersion(None) == indexer.currentVersion()
        assert indexer.thisOrCurrentVersion("*") == indexer.currentVersion()
        assert indexer.thisOrCurrentVersion(VERSION_NONE_NAME) == indexer.currentVersion()
        assert indexer.thisOrCurrentVersion(VERSION_DEFAULT) == VERSION_DEFAULT
        assert indexer.thisOrCurrentVersion(version) == version

    def test_thisOrNextVersion(self):
        version = randint(20, 120)
        indexer = self.initIndexer()
        assert indexer.thisOrNextVersion(None) == indexer.nextVersion()
        assert indexer.thisOrNextVersion("*") == indexer.nextVersion()
        assert indexer.thisOrNextVersion(VERSION_NONE_NAME) == indexer.nextVersion()
        assert indexer.thisOrNextVersion(VERSION_DEFAULT) == VERSION_DEFAULT
        assert indexer.thisOrNextVersion(version) == version

    def test_isValidVersion(self):
        indexer = self.initIndexer()
        # the good
        for i in range(10):
            assert indexer.isValidVersion(randint(2, 120))
        assert indexer.isValidVersion(VERSION_DEFAULT)
        # the bad
        assert not indexer.isValidVersion("bad")
        assert not indexer.isValidVersion("*")
        assert not indexer.isValidVersion(None)
        assert not indexer.isValidVersion(-2)
        assert not indexer.isValidVersion(1.2)

    def test_nextVersion(self):
        # check that the current version advances as expected as
        # both index entries and records are added to the index
        # NOTE all double-calls are deliberate to ensure no change in state on call

        expectedIndex = {}
        indexer = self.initIndexer()
        assert indexer.index == expectedIndex

        # there is no current version
        assert indexer.currentVersion() is None
        assert indexer.currentVersion() is None

        # the first "next" version is the start
        assert indexer.nextVersion() == VERSION_START
        assert indexer.nextVersion() == VERSION_START

        # add an entry to the calibration index
        here = VERSION_START
        # it should be added at the start
        entry = self.indexEntry(indexer.nextVersion())
        indexer.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexer.index == expectedIndex

        # the current version should be this version
        assert indexer.currentVersion() == here
        assert indexer.currentVersion() == here
        # the next version also should be this version
        # until a record is written to disk
        assert indexer.nextVersion() == here
        assert indexer.nextVersion() == here

        # now write the record
        record = self.recordFromIndexEntry(entry)
        indexer.writeRecord(record)

        # the current version hasn't moved
        assert indexer.currentVersion() == here
        # the next version will be one past this one
        assert indexer.nextVersion() == here + 1
        # ensure no change
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1

        # add another entry
        here = here + 1
        # ensure it is added at the next version
        entry = self.indexEntry(indexer.nextVersion())
        indexer.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexer.index == expectedIndex
        assert indexer.currentVersion() == here
        # the next version should be here
        assert indexer.nextVersion() == here
        # now write the record
        indexer.writeRecord(self.recordFromIndexEntry(entry))
        # ensure current still here
        assert indexer.currentVersion() == here
        # ensure next is after here
        assert indexer.nextVersion() == here + 1
        # ensure no change
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1

        # now write a record FIRST, at the next version
        here = here + 1
        record = self.record(here)
        indexer.writeRecord(record)
        # the current version will point here
        assert indexer.currentVersion() == here
        assert indexer.currentVersion() == here
        # the next version will point here
        assert indexer.nextVersion() == here
        assert indexer.nextVersion() == here

        # there is no index entry for this version
        assert indexer.nextVersion() not in indexer.index

        # add the entry
        entry = indexer.indexEntryFromRecord(record)
        indexer.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexer.index == expectedIndex
        # ensure current version points here, next points to next
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1
        assert indexer.currentVersion() == here
        assert indexer.nextVersion() == here + 1

        # write a record first, at a much future version
        # then add an index entry, and ensure it matches
        here = here + 23
        record = self.record(here)
        indexer.writeRecord(record)
        assert indexer.nextVersion() == here
        assert indexer.nextVersion() not in indexer.index

        # now add the entry
        entry = indexer.indexEntryFromRecord(record)
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
        assert indexer.nextVersion() == VERSION_START

        # add an entry at the default version
        entry = self.indexEntry(VERSION_DEFAULT)
        indexer.addIndexEntry(entry)
        expectedIndex[VERSION_DEFAULT] = entry
        assert indexer.index == expectedIndex
        assert entry.version == VERSION_DEFAULT

        # the current version is now the default version
        assert indexer.currentVersion() == VERSION_DEFAULT
        # the next version also should be the default version
        # until a record is written to disk
        assert indexer.nextVersion() == VERSION_DEFAULT

        # now write the record -- it should write to default
        record = self.recordFromIndexEntry(entry)
        indexer.writeRecord(record)
        assert self.recordPath(VERSION_DEFAULT).exists()

        # the current version is still the default version
        assert indexer.currentVersion() == VERSION_DEFAULT
        # the next version will be the starting version
        assert indexer.nextVersion() == VERSION_START

        # add another entry -- now at the start
        entry = self.indexEntry(indexer.nextVersion())
        indexer.addIndexEntry(entry)
        expectedIndex[VERSION_START] = entry
        assert indexer.index == expectedIndex
        assert indexer.currentVersion() == VERSION_START
        # the next version should be the starting version
        # until a record is written
        assert indexer.nextVersion() == VERSION_START
        # now write the record -- ensure it is written at the start
        indexer.writeRecord(self.recordFromIndexEntry(entry))
        assert self.recordPath(VERSION_START).exists()
        # ensure current still here
        assert indexer.currentVersion() == VERSION_START
        # ensure next is after here
        assert indexer.nextVersion() == VERSION_START + 1

    def test_nextVersion_with_default_record_first(self):
        # check default behaves correctly if a record is written first

        indexer = self.initIndexer()
        assert indexer.index == {}

        # there is no current version
        assert indexer.currentVersion() is None

        # the first "next" version is the start
        assert indexer.nextVersion() == VERSION_START

        # add a record at the default version
        record = self.record(VERSION_DEFAULT)
        indexer.writeRecord(record)

        # the current version is now the default version
        assert indexer.currentVersion() == VERSION_DEFAULT
        # the next version also should be the default version
        # until an entry is written to disk
        assert indexer.nextVersion() == VERSION_DEFAULT

        # now write the index entry -- it should write to default
        entry = Record.indexEntryFromRecord(record)
        indexer.addIndexEntry(entry)

        # the current version is still the default version
        assert indexer.currentVersion() == VERSION_DEFAULT
        # the next version will be one past the starting version
        assert indexer.nextVersion() == VERSION_START

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

        # if version path is unitialized, path points to version start
        ans1 = indexer.versionPath(None)
        assert ans1 == self.versionPath(VERSION_START)

        # if version is specified, return that one
        for i in versionList:
            ans3 = indexer.versionPath(i)
            assert ans3 == self.versionPath(i)

    def test_currentPath(self):
        # ensure the current path corresponds to the max in the list of versions
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        indexer.currentPath() == self.versionPath(max(versionList))

    def test_latestApplicablePath(self):
        # ensure latest applicable path corresponds to correct version
        runNumber = "123"
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        # make one entry applicable
        version = 4
        indexer.index[version].appliesTo = f">={runNumber}"
        latest = indexer.latestApplicableVersion(runNumber)
        assert indexer.latestApplicablePath(runNumber) == self.versionPath(latest)

    ### TEST INDEX MANIPULATION METHODS ###

    def test_readIndex(self):
        # test that a previously written index is correctly read
        versionList = [randint(0, 120) for i in range(20)]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())

        indexer = self.initIndexer()
        ans = indexer.readIndex()
        assert ans == index

    def test_readIndex_nothing(self):
        indexer = self.initIndexer()
        assert len(indexer.readIndex()) == 0

    def test_readWriteIndex(self):
        # test that an index can be read/written correctly
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        indexer = self.initIndexer()
        indexer.index = index
        indexer.writeIndex()
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

    def test_addEntry_fails(self):
        # adding an entry with a bad version will fail
        indexer = self.initIndexer()
        indexer.isValidVersion = lambda x: False  # now it will always return false
        entry = self.indexEntry()
        with pytest.raises(RuntimeError):
            indexer.addIndexEntry(entry)

    def test_addEntry_default(self):
        indexer = self.initIndexer()
        entry = self.indexEntry(VERSION_DEFAULT)
        indexer.addIndexEntry(entry)
        assert VERSION_DEFAULT in indexer.index

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

    def test_indexEntryFromRecord(self):
        record = self.record(randint(1, 100))
        indexer = self.initIndexer()
        res = indexer.indexEntryFromRecord(record)
        assert type(res) is IndexEntry
        assert res.runNumber == record.runNumber
        assert res.useLiteMode == record.useLiteMode
        assert res.version == record.version

    def test_indexEntryFromRecord_none(self):
        indexer = self.initIndexer()
        res = indexer.indexEntryFromRecord(None)
        assert res is None

    ### TEST RECORD READ / WRITE METHODS ###

    # read / write #

    def test_readWriteRecord_next_version(self):
        # make sure there exists another version
        # so that we can know it does not default
        # to the starting version
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        nextVersion = indexer.nextVersion()
        assert nextVersion != VERSION_START
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
        assert not self.recordPath(version).exists()
        res = indexer.readRecord(version)
        assert res is None

    def test_readRecord(self):
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexer = self.initIndexer()
        res = indexer.readRecord(record.version)
        assert res == record

    def test_readRecord_invalid_version(self):
        # if an invalid version is attempted to be read, just give the current record

        # NOTE this test assumes no validation is taking place on the arguments to readRecord
        # if validation of the version is ever put in place this test can probably be safely deleted
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexer = self.initIndexer()
        res = indexer.readRecord("*")
        assert res == record

    # write #

    def test_writeRecord_fails(self):
        record = self.record()
        indexer = self.initIndexer()
        indexer.isValidVersion = lambda x: False
        with pytest.raises(RuntimeError):
            indexer.writeRecord(record)

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
        res = Record.parse_file(self.recordPath(version))
        assert res == record
        # ensure the version numbers were set
        assert res.version == version
        assert res.calculationParameters.version == version

    def test_writeRecord_next_version(self):
        # this test we can save to next version

        # make sure there exists other versions so that we can know it does not default to the starting version
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexer = self.initIndexer()
        nextVersion = indexer.nextVersion()
        assert nextVersion != VERSION_START
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
        record = CalibrationRecord.parse_file(Resource.getPath("inputs/calibration/CalibrationRecord_v0001.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexer = self.initIndexer(IndexerType.CALIBRATION)
        indexer.writeRecord(record)
        res = indexer.readRecord(record.version)
        assert type(res) is CalibrationRecord
        assert res == record

    def test_readWriteRecord_normalization(self):
        # prepare the record
        record = NormalizationRecord.parse_file(Resource.getPath("inputs/normalization/NormalizationRecord.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexer = self.initIndexer(IndexerType.NORMALIZATION)
        indexer.writeRecord(record)
        res = indexer.readRecord(record.version)
        assert type(res) is NormalizationRecord
        assert res == record

    def test_readWriteRecord_reduction(self):
        # prepare the record
        record = ReductionRecord.parse_file(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexer = self.initIndexer(IndexerType.REDUCTION)
        indexer.writeRecord(record)
        res = indexer.readRecord(record.version)
        assert type(res) is ReductionRecord
        assert res == record

    ### TEST STATE PARAMETER READ / WRITE METHODS ###

    def test_readParameters_nope(self):
        indexer = self.initIndexer()
        assert not indexer.parametersPath(1).exists()
        with pytest.raises(FileNotFoundError):
            indexer.readParameters(1)

    def test_writeParameters_fails(self):
        params = self.calculationParameters(randint(2, 10))
        indexer = self.initIndexer()
        indexer.isValidVersion = lambda x: False
        with pytest.raises(RuntimeError):
            indexer.writeParameters(params)

    def test_readWriteParameters(self):
        version = randint(1, 10)
        params = self.calculationParameters(version)

        indexer = self.initIndexer()
        indexer.writeParameters(params)
        res = indexer.readParameters(version)
        assert res.version == version
        assert res == params

    def test_readWriteParameters_warn_overwrite(self):
        version = randint(1, 100)

        indexer = self.initIndexer()

        # write some parameters at a version
        params1 = self.calculationParameters(version)
        indexer.writeParameters(params1)
        assert indexer.parametersPath(version).exists()

        # now try to overwrite parameters at same version
        # make sure a warning is logged
        with self.assertLogs(logger=IndexerModule.logger, level=logging.WARNING) as cm:
            params2 = self.calculationParameters(version)
            indexer.writeParameters(params2)
        assert f"Overwriting  parameters at {indexer.parametersPath(version)}" in cm.output[0]

    # make sure the indexer can read/write specific state parameter types #

    def test_readWriteParameters_calibration(self):
        params = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        indexer = self.initIndexer(IndexerType.CALIBRATION)
        indexer.writeParameters(params)
        res = indexer.readParameters()
        assert type(res) is Calibration
        assert res == params

    def test_readWriteParameters_normalization(self):
        params = Normalization.parse_file(Resource.getPath("inputs/normalization/NormalizationParameters.json"))
        indexer = self.initIndexer(IndexerType.NORMALIZATION)
        indexer.writeParameters(params)
        res = indexer.readParameters()
        assert type(res) is Normalization
        assert res == params

    def test_readWriteParameters_reduction(self):
        params = CalculationParameters.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        indexer = self.initIndexer(IndexerType.REDUCTION)
        indexer.writeParameters(params)
        res = indexer.readParameters()
        assert type(res) is CalculationParameters
        assert res == params

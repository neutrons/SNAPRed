# ruff: noqa: E402

import importlib
import tempfile
import unittest
from pathlib import Path
from random import randint
from typing import List

from pydantic import parse_file_as
from snapred.backend.dao.IndexEntry import IndexEntry, Nonentry
from snapred.backend.dao.Record import Record
from snapred.backend.data.Indexor import Indexor, IndexorType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import write_model_list_pretty, write_model_pretty

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

VERSION_START = Config["version..start"]
UNITIALIZED = Config["version.error"]


class TestIndexor(unittest.TestCase):
    ## some helpers for the tests ##

    def setUp(self):
        self.tmpDir = tempfile.TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/")
        self.path = Path(self.tmpDir.name)

    def tearDown(self):
        self.tmpDir.cleanup()

    def initIndexor(self, type=IndexorType.DEFAULT):
        return Indexor(type=type, directory=self.path)

    def indexEntry(self, version):
        return IndexEntry(
            runNumber=randint(1000, 5000),
            useLiteMode=bool(randint(0, 1)),
            version=version,
        )

    def record(self, version, *, runNumber=None):
        if runNumber is None:
            runNumber = randint(1000, 5000)
        return Record(runNumber=runNumber, useLiteMode=bool(randint(0, 1)), version=version)

    def recordFromIndexEntry(self, entry: IndexError) -> Record:
        return Record(
            runNumber=entry.runNumber,
            useLiteMode=entry.useLiteMode,
            version=entry.version,
        )

    def indexPath(self):
        return self.path / "Index.json"

    def versionPath(self, version):
        return self.path / wnvf.fileVersion(version)

    def recordPath(self, version):
        return self.versionPath(version) / "Record.json"

    def makeVersionDir(self, version):
        self.versionPath(version).mkdir()

    def writeRecord(self, record: Record):
        self.makeVersionDir(record.version)
        write_model_pretty(record, self.recordPath(record.version))

    def writeRecordVersion(self, version, *, runNumber=None):
        self.makeVersionDir(version)
        write_model_pretty(self.record(version, runNumber=runNumber), self.recordPath(version))

    ## TESTS OF INITIALIZER ##

    def test_init_nothing(self):
        self.indexor = self.initIndexor()
        assert self.indexor.index == {}
        assert self.indexor.currentVersion() == UNITIALIZED
        assert self.indexor.VERSION_START != UNITIALIZED

    def test_init_versions_exist(self):
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.makeVersionDir(version)

        self.indexor = self.initIndexor()

        assert self.indexor.index == index
        assert self.indexor.currentVersion() == max(versionList)

    def test_init_versions_missing_index(self):
        # create a situation where the index is missing a value shown in the directory tree
        # then the indexor should create an index entry based off of the directory record
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

        self.indexor = self.initIndexor()

        # what we expect -- an index with version 1, 2, 3, 4
        # version 3 will be created from the record
        # the timestamps will necessarily differ, so set them both to zero
        expectedIndex = index.copy()
        expectedIndex[3] = self.indexor.indexEntryFromRecord(self.indexor.readRecord(3))
        expectedIndex[3].timestamp = 0
        self.indexor.index[3].timestamp = 0

        assert self.indexor.index == expectedIndex

    def test_init_versions_missing_directory(self):
        # create a situation where the index has a value not reflected in directory tree
        # then the indexor should delete that index entry
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        # remove version 3
        versionList.remove(3)
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)

        self.indexor = self.initIndexor()

        # what we expect -- an index with version 1, 2, 4
        # version 3 will have been deleted
        expectedIndex = index.copy()
        del expectedIndex[3]

        assert self.indexor.index == expectedIndex

    def test_delete_save_on_exit(self):
        versionList = [1, 2, 3]
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)
        assert not self.indexPath().exists()

        self.indexor = self.initIndexor()
        del self.indexor
        assert self.indexPath().exists()
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

    def test_delete_reconcile_versions(self):
        versionList = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)

        # add an index entry but don't save a file
        # the index will not have the next version
        self.indexor = self.initIndexor()
        self.indexor.addIndexEntry(self.indexEntry(4))
        del self.indexor
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

        # add a record but not an index
        # the index will have a version created from the record
        self.indexor = self.initIndexor()
        self.writeRecordVersion(4)
        del self.indexor
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList) + 1

    ### TEST VERSION GETTER METHODS ###

    def test_defaultVersion_calibration(self):
        self.indexor = self.initIndexor(type=IndexorType.CALIBRATION)
        assert self.indexor.defaultVersion() == Config["version.calibration.default"]

    def test_defaultVersion_normalization(self):
        # Normalization versions do not have a default.
        self.indexor = self.initIndexor(type=IndexorType.NORMALIZATION)
        assert self.indexor.defaultVersion() == Config["version.error"]

    def test_currentVersion_none(self):
        self.indexor = self.initIndexor()
        assert self.indexor.currentVersion() == UNITIALIZED
        self.indexor.currentPath() == self.versionPath(VERSION_START)

    def test_currentVersion_add(self):
        versions = [2, 3, 4]
        index = {version: self.indexEntry(version) for version in versions}
        # prepare directories for the versions
        for version in versions:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        # ensure we are in position we expect:
        assert list(self.indexor.index.keys()) == versions
        assert list(self.indexor.readDirectoryList()) == versions
        # now check that the current version advances when an entry is added
        assert self.indexor.currentVersion() == max(versions)
        self.indexor.addIndexEntry(self.indexEntry(randint(1, 5)))
        assert self.indexor.currentVersion() == max(versions) + 1

    def test_currentVersion_dirHigher(self):
        dirVersions = [1, 2, 3]
        indexVersions = [1, 2]
        index = {version: self.indexEntry(version) for version in indexVersions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in dirVersions:
            self.writeRecordVersion(version)

        self.indexor = self.initIndexor()
        assert self.indexor.currentVersion() == max(dirVersions)

    def test_currentVersion_indexhigher(self):
        dirVersions = [1, 2]
        indexVersions = [1, 2, 3]
        index = {version: self.indexEntry(version) for version in indexVersions}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in dirVersions:
            self.writeRecordVersion(version)

        self.indexor = self.initIndexor()
        assert self.indexor.currentVersion() == max(dirVersions)

    def test_nextVersion(self):
        # NOTE all double-calls are deliberate to ensure no change in state on call

        expectedIndex = {}
        self.indexor = self.initIndexor()
        assert self.indexor.index == expectedIndex

        # there is no current version
        assert self.indexor.currentVersion() == UNITIALIZED
        assert self.indexor.currentVersion() == UNITIALIZED

        # the first "next" version is the start
        assert self.indexor.nextVersion() == VERSION_START
        assert self.indexor.nextVersion() == VERSION_START

        # add an entry to the calibration index
        here = VERSION_START
        # it should be added at the start
        entry = self.indexEntry(3)
        self.indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert self.indexor.index == expectedIndex

        # the current version should be this version
        assert self.indexor.currentVersion() == here
        assert self.indexor.currentVersion() == here
        # the next version should be this version
        # until a record is written to disk
        assert self.indexor.nextVersion() == here
        assert self.indexor.nextVersion() == here

        # now write the record
        record = self.recordFromIndexEntry(entry)
        self.writeRecord(record)

        # the current version hasn't moved
        assert self.indexor.currentVersion() == here
        # the next version will be one past this one
        assert self.indexor.nextVersion() == here + 1
        # ensure no change
        assert self.indexor.currentVersion() == here
        assert self.indexor.nextVersion() == here + 1

        # add another entry
        here = here + 1
        # ensure it is added at the next version
        entry = self.indexEntry(3)
        self.indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert self.indexor.index == expectedIndex
        assert self.indexor.currentVersion() == here
        # the next version should be here
        assert self.indexor.nextVersion() == here
        # now write the record
        self.writeRecord(self.recordFromIndexEntry(entry))
        # ensure current still here
        assert self.indexor.currentVersion() == here
        # ensure next is after here
        assert self.indexor.nextVersion() == here + 1
        # ensure no change
        assert self.indexor.currentVersion() == here
        assert self.indexor.nextVersion() == here + 1

        # now write a record FIRST, at the next version
        here = here + 1
        record = self.record(here)
        self.writeRecord(record)
        # the next version will point here
        assert self.indexor.nextVersion() == here
        assert self.indexor.nextVersion() == here

        # there is no index for this version
        assert self.indexor.nextVersion() not in self.indexor.index

        # add the entry
        entry = self.indexor.indexEntryFromRecord(record)
        expectedIndex[here] = entry
        self.indexor.addIndexEntry(entry)
        assert self.indexor.index == expectedIndex
        # ensure current version points here, next points to next
        assert self.indexor.currentVersion() == here
        assert self.indexor.nextVersion() == here + 1
        assert self.indexor.currentVersion() == here
        assert self.indexor.nextVersion() == here + 1

        # write a record first, at a much future version
        # then add an index entry, and ensure it matches
        here = here + 23
        record = self.record(here)
        self.writeRecord(record)
        assert self.indexor.nextVersion() == here
        assert self.indexor.nextVersion() not in self.indexor.index

        # now add the entry
        entry = self.indexor.indexEntryFromRecord(record)
        expectedIndex[here] = entry
        self.indexor.addIndexEntry(entry)
        assert self.indexor.index == expectedIndex
        # enssure match
        assert self.indexor.currentVersion() == here
        assert self.indexor.nextVersion() == here + 1

    ### TESTS OF VERSION COMPARISON METHODS ###

    def test__isApplicableEntry_equals(self):
        self.indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "123"
        assert self.indexor._isApplicableEntry(entry, "123")

    def test__isApplicableEntry_greaterThan(self):
        self.indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">123"
        assert self.indexor._isApplicableEntry(entry, "456")

    def test__isApplicableEntry_lessThan(self):
        self.indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<123"
        assert self.indexor._isApplicableEntry(entry, "99")

    def test_isApplicableEntry_lessThanEquals(self):
        self.indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<=123"
        assert self.indexor._isApplicableEntry(entry, "123")
        assert self.indexor._isApplicableEntry(entry, "99")
        assert not self.indexor._isApplicableEntry(entry, "456")

    def test_isApplicableEntry_greaterThanEquals(self):
        self.indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">=123"
        assert self.indexor._isApplicableEntry(entry, "123")
        assert self.indexor._isApplicableEntry(entry, "456")
        assert not self.indexor._isApplicableEntry(entry, "99")

    ### TESTS OF PATH METHODS ###

    def test_indexPath(self):
        self.indexor = self.initIndexor()
        assert self.indexor.indexPath() == self.indexPath()

    def test_recordPath(self):
        self.indexor = self.initIndexor()
        assert self.indexor.recordPath(12) == self.recordPath(12)

    def test_versionPath(self):
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()

        # if version path is unitialized, path points to version start
        ans1 = self.indexor.versionPath(UNITIALIZED)
        assert ans1 == self.versionPath(VERSION_START)

        # if version is "*" return current
        ans2 = self.indexor.versionPath("*")
        assert ans2 == self.versionPath(max(versionList))

        # if version isspecified, return that one
        for i in versionList:
            ans3 = self.indexor.versionPath(i)
            assert ans3 == self.versionPath(i)

    def test_currentPath(self):
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        self.indexor.currentPath() == self.versionPath(max(versionList))

    def test_latestApplicablePath(self):
        runNumber = "123"
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        # make one entry applicable
        version = 4
        self.indexor.index[version].appliesTo = f">={runNumber}"
        latest = self.indexor.latestApplicableVersion(runNumber)
        assert self.indexor.latestApplicablePath(runNumber) == self.versionPath(latest)

    ### TEST INDEX MANIPULATION METHODS ###

    def test_readIndex(self):
        versionList = [randint(0, 120) for i in range(20)]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())

        self.indexor = self.initIndexor()
        ans = self.indexor.readIndex()
        assert ans == index

    def test_readWriteIndex(self):
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        self.indexor = self.initIndexor()
        self.indexor.index = index
        self.indexor.writeIndex()
        ans = self.indexor.readIndex()
        assert ans == index

    def test_addEntry_to_nothing(self):
        self.indexor = self.initIndexor()
        assert self.indexor.index == {}
        assert self.indexor.currentVersion() == UNITIALIZED
        self.indexor.addIndexEntry(self.indexEntry(3))
        assert self.indexor.currentVersion() != UNITIALIZED
        # add one more time to make sure no conflicts with things not existing
        self.indexor.addIndexEntry(self.indexEntry(4))

    def test_addEntry_writes(self):
        self.indexor = self.initIndexor()
        for i in range(3, 10):
            self.indexor.addIndexEntry(self.indexEntry(i))
            readIndex = parse_file_as(List[IndexEntry], self.indexor.indexPath())
            assert readIndex == list(self.indexor.index.values())

    def test_addEntry_advances(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        assert self.indexor.currentVersion() == max(versionList)
        self.indexor.addIndexEntry(self.indexEntry(3))
        assert self.indexor.currentVersion() == max(versionList) + 1

    def test_addEntry_at_version_new(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        assert 3 not in self.indexor.index
        self.indexor.addIndexEntry(self.indexEntry(3), 3)
        assert self.indexor.index[3] is not Nonentry

    def test_addEntry_at_version_overwrite(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        self.indexor = self.initIndexor()
        entry7 = self.indexor.index[7]
        self.indexor.addIndexEntry(self.indexEntry(3), 7)
        assert self.indexor.index[7] is not entry7

    def test_indexEntryFromRecord(self):
        record = self.record(107)
        self.indexor = self.initIndexor()
        res = self.indexor.indexEntryFromRecord(record)
        assert type(res) is IndexEntry
        assert res.runNumber == record.runNumber
        assert res.useLiteMode == record.useLiteMode
        assert res.version == record.version

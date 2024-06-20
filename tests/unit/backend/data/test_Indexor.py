# ruff: noqa: E402

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
from snapred.backend.dao.indexing.IndexEntry import IndexEntry, Nonentry
from snapred.backend.dao.indexing.Record import Nonrecord, Record
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VERSION_NONE, VERSION_START
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.data.Indexor import Indexor, IndexorType
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

IndexorModule = importlib.import_module(Indexor.__module__)


class TestIndexor(unittest.TestCase):
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

    def initIndexor(self, indexorType=IndexorType.DEFAULT):
        # create an indexor of specific type inside the temporrary directory
        return Indexor(indexorType=indexorType, directory=self.path)

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
        # write a record independently of the indexor
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
        indexor = self.initIndexor()
        assert indexor.index == {}
        assert indexor.currentVersion() is None

    def test_init_versions_exist(self):
        # when initialized, existing information is loaded
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.makeVersionDir(version)

        indexor = self.initIndexor()

        assert indexor.index == index
        assert indexor.currentVersion() == max(versionList)

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

        indexor = self.initIndexor()

        # what we expect -- an index with version 1, 2, 3, 4
        # version 3 will be created from the record
        # the timestamps will necessarily differ, so set them both to zero
        expectedIndex = index.copy()
        expectedIndex[3] = indexor.indexEntryFromRecord(indexor.readRecord(3))
        expectedIndex[3].timestamp = 0
        indexor.index[3].timestamp = 0

        assert indexor.index == expectedIndex

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

        indexor = self.initIndexor()

        # what we expect -- an index with version 1, 2, 4
        # version 3 will have been deleted
        expectedIndex = index.copy()
        del expectedIndex[3]

        assert indexor.index == expectedIndex

    def test_delete_save_on_exit(self):
        # ensure the index list is saved when the indexor is deleted
        versionList = [1, 2, 3]
        # now add all the needed files as before
        for version in versionList:
            self.writeRecordVersion(version)
        assert not self.indexPath().exists()

        indexor = self.initIndexor()
        del indexor
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
        indexor = self.initIndexor()
        indexor.addIndexEntry(self.indexEntry(4))
        del indexor
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList)

        # add a record but not an index
        # the index will have a version created from the record
        indexor = self.initIndexor()
        self.writeRecordVersion(4)
        del indexor
        savedIndex = parse_file_as(List[IndexEntry], self.indexPath())
        assert len(savedIndex) == len(versionList) + 1

    ### TEST VERSION GETTER METHODS ###

    def test_defaultVersion(self):
        indexor = self.initIndexor()
        assert indexor.defaultVersion() == VERSION_DEFAULT

    def test_currentVersion_none(self):
        # ensure the current version of an empty index is unitialized
        indexor = self.initIndexor()
        assert indexor.currentVersion() is None
        # the path should go to the starting version
        indexor.currentPath() == self.versionPath(VERSION_START)

    def test_currentVersion_add(self):
        # ensure current version advances when index entries are written
        # prepare directories for the versions
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        # ensure we are in position we expect:
        assert list(indexor.index.keys()) == versions
        assert list(indexor.readDirectoryList()) == versions
        # now check that the current version advances when an entry is added
        assert indexor.currentVersion() == max(versions)
        indexor.addIndexEntry(self.indexEntry(randint(1, 5)))
        assert indexor.currentVersion() == max(versions) + 1

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

        indexor = self.initIndexor()
        assert indexor.currentVersion() == max(dirVersions)

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

        indexor = self.initIndexor()
        assert indexor.currentVersion() == max(dirVersions)

    def test_thisOrCurrentVersion(self):
        version = randint(20, 120)
        indexor = self.initIndexor()
        assert indexor.thisOrCurrentVersion(None) == indexor.currentVersion()
        assert indexor.thisOrCurrentVersion("*") == indexor.currentVersion()
        assert indexor.thisOrCurrentVersion(VERSION_NONE) == indexor.currentVersion()
        assert indexor.thisOrCurrentVersion(VERSION_DEFAULT) == VERSION_DEFAULT
        assert indexor.thisOrCurrentVersion(version) == version

    def test_thisOrNextVersion(self):
        version = randint(20, 120)
        indexor = self.initIndexor()
        assert indexor.thisOrNextVersion(None) == indexor.nextVersion()
        assert indexor.thisOrNextVersion("*") == indexor.nextVersion()
        assert indexor.thisOrNextVersion(VERSION_NONE) == indexor.nextVersion()
        assert indexor.thisOrNextVersion(VERSION_DEFAULT) == VERSION_DEFAULT
        assert indexor.thisOrNextVersion(version) == version

    def test_nextVersion(self):
        # check that the current version advances as expected as
        # both index entries and records are added to the index
        # NOTE all double-calls are deliberate to ensure no change in state on call

        expectedIndex = {}
        indexor = self.initIndexor()
        assert indexor.index == expectedIndex

        # there is no current version
        assert indexor.currentVersion() is None
        assert indexor.currentVersion() is None

        # the first "next" version is the start
        assert indexor.nextVersion() == VERSION_START
        assert indexor.nextVersion() == VERSION_START

        # add an entry to the calibration index
        here = VERSION_START
        # it should be added at the start
        entry = self.indexEntry()
        indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexor.index == expectedIndex

        # the current version should be this version
        assert indexor.currentVersion() == here
        assert indexor.currentVersion() == here
        # the next version also should be this version
        # until a record is written to disk
        assert indexor.nextVersion() == here
        assert indexor.nextVersion() == here

        # now write the record
        record = self.recordFromIndexEntry(entry)
        indexor.writeRecord(record)

        # the current version hasn't moved
        assert indexor.currentVersion() == here
        # the next version will be one past this one
        assert indexor.nextVersion() == here + 1
        # ensure no change
        assert indexor.currentVersion() == here
        assert indexor.nextVersion() == here + 1

        # add another entry
        here = here + 1
        # ensure it is added at the next version
        entry = self.indexEntry()
        indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexor.index == expectedIndex
        assert indexor.currentVersion() == here
        # the next version should be here
        assert indexor.nextVersion() == here
        # now write the record
        indexor.writeRecord(self.recordFromIndexEntry(entry))
        # ensure current still here
        assert indexor.currentVersion() == here
        # ensure next is after here
        assert indexor.nextVersion() == here + 1
        # ensure no change
        assert indexor.currentVersion() == here
        assert indexor.nextVersion() == here + 1

        # now write a record FIRST, at the next version
        here = here + 1
        record = self.record(here)
        indexor.writeRecord(record)
        # the current version will point here
        assert indexor.currentVersion() == here
        assert indexor.currentVersion() == here
        # the next version will point here
        assert indexor.nextVersion() == here
        assert indexor.nextVersion() == here

        # there is no index entry for this version
        assert indexor.nextVersion() not in indexor.index

        # add the entry
        entry = indexor.indexEntryFromRecord(record)
        indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexor.index == expectedIndex
        # ensure current version points here, next points to next
        assert indexor.currentVersion() == here
        assert indexor.nextVersion() == here + 1
        assert indexor.currentVersion() == here
        assert indexor.nextVersion() == here + 1

        # write a record first, at a much future version
        # then add an index entry, and ensure it matches
        here = here + 23
        record = self.record()
        indexor.writeRecord(record, here)
        assert indexor.nextVersion() == here
        assert indexor.nextVersion() not in indexor.index

        # now add the entry
        entry = indexor.indexEntryFromRecord(record)
        indexor.addIndexEntry(entry)
        expectedIndex[here] = entry
        assert indexor.index == expectedIndex
        # enssure match
        assert indexor.currentVersion() == here
        assert indexor.nextVersion() == here + 1

    def test_nextVersion_with_default_index_first(self):
        # check that indexor handles the default version as expected

        expectedIndex = {}
        indexor = self.initIndexor()
        assert indexor.index == expectedIndex

        # there is no current version
        assert indexor.currentVersion() is None

        # the first "next" version is the start
        assert indexor.nextVersion() == VERSION_START

        # add an entry at the default version
        entry = self.indexEntry()
        indexor.addIndexEntry(entry, VERSION_DEFAULT)
        expectedIndex[VERSION_DEFAULT] = entry
        assert indexor.index == expectedIndex
        assert entry.version == VERSION_DEFAULT

        # the current version is now the default version
        assert indexor.currentVersion() == VERSION_DEFAULT
        # the next version also should be the default version
        # until a record is written to disk
        assert indexor.nextVersion() == VERSION_DEFAULT

        # now write the record -- it should write to default
        record = self.recordFromIndexEntry(entry)
        indexor.writeRecord(record)
        assert self.recordPath(VERSION_DEFAULT).exists()

        # the current version is still the default version
        assert indexor.currentVersion() == VERSION_DEFAULT
        # the next version will be the starting version
        assert indexor.nextVersion() == VERSION_START

        # add another entry -- now at the start
        entry = self.indexEntry()
        indexor.addIndexEntry(entry)
        expectedIndex[VERSION_START] = entry
        assert indexor.index == expectedIndex
        assert indexor.currentVersion() == VERSION_START
        # the next version should be the starting version
        # until a record is written
        assert indexor.nextVersion() == VERSION_START
        # now write the record -- ensure it is written at the start
        indexor.writeRecord(self.recordFromIndexEntry(entry))
        assert self.recordPath(VERSION_START).exists()
        # ensure current still here
        assert indexor.currentVersion() == VERSION_START
        # ensure next is after here
        assert indexor.nextVersion() == VERSION_START + 1

    def test_nextVersion_with_default_record_first(self):
        # check default behaves correctly if a record is written first

        indexor = self.initIndexor()
        assert indexor.index == {}

        # there is no current version
        assert indexor.currentVersion() is None

        # the first "next" version is the start
        assert indexor.nextVersion() == VERSION_START

        # add a record at the default version
        record = self.record()
        indexor.writeRecord(record, VERSION_DEFAULT)

        # the current version is now the default version
        assert indexor.currentVersion() == VERSION_DEFAULT
        # the next version also should be the default version
        # until an entry is written to disk
        assert indexor.nextVersion() == VERSION_DEFAULT

        # now write the index entry -- it should write to default
        entry = Record.indexEntryFromRecord(record)
        indexor.addIndexEntry(entry)

        # the current version is still the default version
        assert indexor.currentVersion() == VERSION_DEFAULT
        # the next version will be one past the starting version
        assert indexor.nextVersion() == VERSION_START

    ### TESTS OF VERSION COMPARISON METHODS ###

    def test__isApplicableEntry_equals(self):
        indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "123"
        assert indexor._isApplicableEntry(entry, "123")

    def test__isApplicableEntry_greaterThan(self):
        indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">123"
        assert indexor._isApplicableEntry(entry, "456")

    def test__isApplicableEntry_lessThan(self):
        indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<123"
        assert indexor._isApplicableEntry(entry, "99")

    def test_isApplicableEntry_lessThanEquals(self):
        indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = "<=123"
        assert indexor._isApplicableEntry(entry, "123")
        assert indexor._isApplicableEntry(entry, "99")
        assert not indexor._isApplicableEntry(entry, "456")

    def test_isApplicableEntry_greaterThanEquals(self):
        indexor = self.initIndexor()
        entry = self.indexEntry(version=0)
        entry.appliesTo = ">=123"
        assert indexor._isApplicableEntry(entry, "123")
        assert indexor._isApplicableEntry(entry, "456")
        assert not indexor._isApplicableEntry(entry, "99")

    ### TESTS OF PATH METHODS ###

    def test_indexPath(self):
        indexor = self.initIndexor()
        assert indexor.indexPath() == self.indexPath()

    def test_recordPath(self):
        indexor = self.initIndexor()
        assert indexor.recordPath(12) == self.recordPath(12)

    def test_versionPath(self):
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()

        # if version path is unitialized, path points to version start
        ans1 = indexor.versionPath(None)
        assert ans1 == self.versionPath(VERSION_START)

        # if version is specified, return that one
        for i in versionList:
            ans3 = indexor.versionPath(i)
            assert ans3 == self.versionPath(i)

    def test_currentPath(self):
        # ensure the current path corresponds to the max in the list of versions
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        indexor.currentPath() == self.versionPath(max(versionList))

    def test_latestApplicablePath(self):
        # ensure latest applicable path corresponds to correct version
        runNumber = "123"
        versionList = [3, 4, 5]
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        # make one entry applicable
        version = 4
        indexor.index[version].appliesTo = f">={runNumber}"
        latest = indexor.latestApplicableVersion(runNumber)
        assert indexor.latestApplicablePath(runNumber) == self.versionPath(latest)

    ### TEST INDEX MANIPULATION METHODS ###

    def test_readIndex(self):
        # test that a previously written index is correctly read
        versionList = [randint(0, 120) for i in range(20)]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(list(index.values()), self.indexPath())

        indexor = self.initIndexor()
        ans = indexor.readIndex()
        assert ans == index

    def test_readIndex_nothing(self):
        indexor = self.initIndexor()
        assert len(indexor.readIndex()) == 0

    def test_readWriteIndex(self):
        # test that an index can be read/written correctly
        versionList = [1, 2, 3, 4]
        index = {version: self.indexEntry(version) for version in versionList}
        indexor = self.initIndexor()
        indexor.index = index
        indexor.writeIndex()
        ans = indexor.readIndex()
        assert ans == index

    def test_addEntry_to_nothing(self):
        # adding an index entry to an empty index works
        indexor = self.initIndexor()
        assert indexor.index == {}
        assert indexor.currentVersion() is None
        indexor.addIndexEntry(self.indexEntry(3))
        assert indexor.currentVersion() is not None
        # add one more time to make sure no conflicts with things not existing
        indexor.addIndexEntry(self.indexEntry(4))

    def test_addEntry_writes(self):
        # adding an index entry also writes the index entry to disk
        indexor = self.initIndexor()
        for i in range(3, 10):
            indexor.addIndexEntry(self.indexEntry(i))
            readIndex = parse_file_as(List[IndexEntry], indexor.indexPath())
            assert readIndex == list(indexor.index.values())

    def test_addEntry_advances(self):
        # adding an index entry advances the current version
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        assert indexor.currentVersion() == max(versionList)
        indexor.addIndexEntry(self.indexEntry(3))
        assert indexor.currentVersion() == max(versionList) + 1

    def test_addEntry_at_version_new(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        assert 3 not in indexor.index
        indexor.addIndexEntry(self.indexEntry(3), 3)
        assert indexor.index[3] is not Nonentry

    def test_addEntry_at_version_overwrite(self):
        versionList = [2, 7, 11]
        index = {version: self.indexEntry(version) for version in versionList}
        write_model_list_pretty(index.values(), self.indexPath())
        for version in versionList:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        entry7 = indexor.index[7]
        indexor.addIndexEntry(self.indexEntry(3), 7)
        assert indexor.index[7] is not entry7

    def test_indexEntryFromRecord(self):
        record = self.record(randint(1, 100))
        indexor = self.initIndexor()
        res = indexor.indexEntryFromRecord(record)
        assert type(res) is IndexEntry
        assert res.runNumber == record.runNumber
        assert res.useLiteMode == record.useLiteMode
        assert res.version == record.version

    def test_indexEntryFromRecord_none(self):
        indexor = self.initIndexor()
        res = indexor.indexEntryFromRecord(Nonrecord)
        assert res is Nonentry

    ### TEST RECORD READ / WRITE METHODS ###

    # read / write #

    def test_readWriteRecord_no_version(self):
        # make sure there exists another version
        # so that we can know it does not default
        # to the starting version
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        nextVersion = indexor.nextVersion()
        assert nextVersion != VERSION_START
        # now write then read the record
        # make sure the record was saved at the next version
        # and the read / written records match
        record = self.record(randint(6, 16))
        indexor.writeRecord(record)
        res = indexor.readRecord(nextVersion)
        assert record.version == nextVersion
        assert res == record

    def test_readWriteRecord_with_version(self):
        # write a record at some version number
        record = self.record(randint(10, 20))
        indexor = self.initIndexor()
        version = randint(21, 30)
        # write then read the record
        # make sure the record version was updated
        # and the read / written records match
        indexor.writeRecord(record, version)
        res = indexor.readRecord(version)
        assert record.version == version
        assert res == record

    # read #

    def test_readRecord_none(self):
        version = randint(1, 11)
        indexor = self.initIndexor()
        assert not self.recordPath(version).exists()
        res = indexor.readRecord(version)
        assert res is Nonrecord

    def test_readRecord(self):
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexor = self.initIndexor()
        res = indexor.readRecord(record.version)
        assert res == record

    def test_readRecord_no_version(self):
        # NOTE this test assumes no validation is taking place
        # if validation of the version is ever put in place
        # this test can probably be safely deleted
        record = self.record(randint(1, 100))
        self.writeRecord(record)
        indexor = self.initIndexor()
        res = indexor.readRecord("*")
        assert res == record

    # write #

    def test_writeRecord_with_version(self):
        # this test ensures a record can be written to the indicated version
        # create a record and write it
        record = self.record(randint(1, 10))
        indexor = self.initIndexor()
        version = randint(11, 20)
        indexor.writeRecord(record, version)
        assert record.version == version
        assert self.recordPath(version).exists()
        # read it back in and ensure it is the same
        res = Record.parse_file(self.recordPath(version))
        assert res == record
        # ensure the version numbers were set
        assert res.version == version
        assert res.calculationParameters.version == version

    def test_writeRecord_no_version(self):
        # this test ensures no version defaults to next version

        # make sure there exists other versions so that we can know it does not default to the starting version
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        nextVersion = indexor.nextVersion()
        assert nextVersion != VERSION_START
        # now write the record
        record = self.record(randint(10, 20))
        indexor.writeRecord(record)
        assert record.version == nextVersion
        assert self.recordPath(nextVersion).exists()
        res = parse_file_as(Record, self.recordPath(nextVersion))
        assert res == record
        # ensure the version numbers were set
        assert res.version == nextVersion
        assert res.calculationParameters.version == nextVersion

    def test_writeRecord_star(self):
        # this test ensures that an invalid version defaults to next version

        # make sure there exists other versions so that we can know it does not default o the starting version
        versions = [2, 3, 4]
        for version in versions:
            self.writeRecordVersion(version)
        indexor = self.initIndexor()
        nextVersion = indexor.nextVersion()
        assert nextVersion != VERSION_START
        # now write the record
        record = self.record(randint(10, 20))
        indexor.writeRecord(record, "*")
        assert record.version == nextVersion
        assert self.recordPath(nextVersion).exists()
        res = parse_file_as(Record, self.recordPath(nextVersion))
        assert res == record
        assert res.version == nextVersion
        assert res.calculationParameters.version == nextVersion

    # make sure the indexor can read/write specific record types #

    def test_readWriteRecord_calibration(self):
        # prepare the record
        record = CalibrationRecord.parse_file(Resource.getPath("inputs/calibration/CalibrationRecord_v0001.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexor = self.initIndexor(IndexorType.CALIBRATION)
        indexor.writeRecord(record)
        res = indexor.readRecord(record.version)
        assert type(res) is CalibrationRecord
        assert res == record

    def test_readWriteRecord_normalization(self):
        # prepare the record
        record = NormalizationRecord.parse_file(Resource.getPath("inputs/normalization/NormalizationRecord.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexor = self.initIndexor(IndexorType.NORMALIZATION)
        indexor.writeRecord(record)
        res = indexor.readRecord(record.version)
        assert type(res) is NormalizationRecord
        assert res == record

    def test_readWriteRecord_reduction(self):
        # prepare the record
        record = ReductionRecord.parse_file(Resource.getPath("inputs/reduction/ReductionRecord_v0001.json"))
        record.version = randint(2, 100)
        # write then read in the record
        indexor = self.initIndexor(IndexorType.REDUCTION)
        indexor.writeRecord(record)
        res = indexor.readRecord(record.version)
        assert type(res) is ReductionRecord
        assert res == record

    ### TEST STATE PARAMETER READ / WRITE METHODS ###

    def test_readParameters_nope(self):
        indexor = self.initIndexor()
        assert not indexor.parametersPath(1).exists()
        with pytest.raises(FileNotFoundError):
            indexor.readParameters(1)

    def test_readWriteParameters_with_version(self):
        version = randint(1, 10)
        params = self.calculationParameters(version)

        indexor = self.initIndexor()
        version = randint(11, 20)
        indexor.writeParameters(params, version)
        res = indexor.readParameters(version)
        assert res.version == version
        assert res == params

    def test_readWriteParameters_no_version(self):
        version = randint(1, 10)
        params = self.calculationParameters(version)

        indexor = self.initIndexor()
        indexor.index = {randint(11, 20): Nonentry}
        nextVersion = indexor.nextVersion()
        indexor.writeParameters(params)
        res = indexor.readParameters()
        assert res.version == nextVersion
        assert res == params

    def test_readWriteParameters_warn_overwrite(self):
        version = randint(1, 100)

        indexor = self.initIndexor()

        # write some parameters at a version
        params1 = self.calculationParameters(version)
        indexor.writeParameters(params1, version)
        assert indexor.parametersPath(version).exists()

        # now try to overwrite parameters at same version
        # make sure a warning is logged
        with self.assertLogs(logger=IndexorModule.logger, level=logging.WARNING) as cm:
            params2 = self.calculationParameters(version)
            indexor.writeParameters(params2, version)
        assert f"Overwriting  parameters at {indexor.parametersPath(version)}" in cm.output[0]

    # make sure the indexor can read/write specific state parameter types #

    def test_readWriteParameters_calibration(self):
        params = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        indexor = self.initIndexor(IndexorType.CALIBRATION)
        indexor.writeParameters(params)
        res = indexor.readParameters()
        assert type(res) is Calibration
        assert res == params

    def test_readWriteParameters_normalization(self):
        params = Normalization.parse_file(Resource.getPath("inputs/normalization/NormalizationParameters.json"))
        indexor = self.initIndexor(IndexorType.NORMALIZATION)
        indexor.writeParameters(params)
        res = indexor.readParameters()
        assert type(res) is Normalization
        assert res == params

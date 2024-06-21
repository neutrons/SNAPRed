import os
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import validate_call

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT, VERSION_DEFAULT_NAME, VERSION_START
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.log.logger import snapredLogger
from snapred.meta.mantid.AllowedPeakTypes import StrEnum
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

logger = snapredLogger.getLogger(__name__)


"""
    The Indexor will automatically track versions and
    This is intended to have responsibility over one state/resolution/workflow combination,
    (e.g state abcd123efg, lite mode, calibration), and for that combination will keep tabs on
    the index and the directory tree to determine appropriate version numbers.

    All saving or loading of indices, records, or calculation parameter files should be
    handled through the appropriate Indexor.

    Saving will automatically handle applying the corrct version number in all cases.
    If a specific version is specified, all saving will be to that version; if None is passed
    instead, then saving will be to the next version according to the Indexor.

    When saving, the Indexor will overwrite and ignore any versions attached to an object.

    The Indexor version list will only update when both a record and a corresponding inex entry
    have been written.
"""


class IndexorType(StrEnum):
    DEFAULT = ""
    CALIBRATION = "Calibration"
    NORMALIZATION = "Normalization"
    REDUCTION = "Reduction"


class Indexor:
    rootDirectory: Path
    index: Dict[int, IndexEntry]

    indexorType: IndexorType

    ## CONSTRUCTOR / DESTRUCTOR METHODS ##

    @validate_call
    def __init__(self, *, indexorType: IndexorType, directory: Path | str) -> None:
        self.indexorType = indexorType
        self.rootDirectory = Path(directory)
        self.index = self.readIndex()
        self.dirVersions = self.readDirectoryList()
        self.reconcileIndexToFiles()

    def __del__(self):
        # define the index to automatically write itself whenever the program closes
        if self.rootDirectory.exists():
            self.reconcileIndexToFiles()
            self.writeIndex()

    def readDirectoryList(self):
        # create the directory version list from the directory structure
        versions = set()
        for fname in self.rootDirectory.glob("v_*"):
            if os.path.isdir(fname):
                version = str(fname).split("_")[-1]
                if version == VERSION_DEFAULT_NAME:
                    version = VERSION_DEFAULT
                version = int(version)
                versions.add(version)
        return versions

    def reconcileIndexToFiles(self):
        # if a directory has no entry in the index, create one for it
        versions = self.readDirectoryList()
        for version in versions:
            if version not in self.index:
                record = self.readRecord(version)
                self.index[version] = self.indexEntryFromRecord(record)
        # if a version exists in the index with no corresponding directory, delete it
        it_is_truly_necessary_to_make_this_list = list(self.index.keys())
        for version in it_is_truly_necessary_to_make_this_list:
            if version not in versions:
                del self.index[version]

    ## VERSION GETTERS ##

    def allVersions(self) -> List[int]:
        return list(self.index.keys())

    def defaultVersion(self) -> int:
        """
        The version number to use for default states.
        """
        return VERSION_DEFAULT

    def currentVersion(self) -> int:
        """
        The largest version found by the Indexor.
        """
        currentVersion = None
        overlap = set.union(set(self.index.keys()), self.dirVersions)
        if len(overlap) == 0:
            currentVersion = None
        elif len(overlap) == 1:
            currentVersion = list(overlap)[0]
        else:
            versions = [version for version in overlap if isinstance(version, int)]
            currentVersion = max(versions)
        return currentVersion

    def latestApplicableVersion(self, runNumber: str) -> int:
        """
        The most recent version in time, which is applicable to the run number.
        """
        # sort by timestamp
        entries = list(self.index.values())
        entries.sort(key=lambda x: x.timestamp)
        # filter for latest applicable
        relevantEntries = list(filter(lambda x: self._isApplicableEntry(x, runNumber), entries))
        if len(relevantEntries) < 1:
            version = None
        elif len(relevantEntries) == 1:
            version = relevantEntries[0].version
        else:
            if VERSION_DEFAULT in self.index:
                relevantEntries.remove(self.index[VERSION_DEFAULT])
            version = relevantEntries[-1].version
        return version

    def nextVersion(self) -> int:
        """
        A new version number to use for saving calibration records.
        """

        version = None

        # if the index and directories are in sync, the next version is one past them
        if set(self.index.keys()) == self.dirVersions:
            # remove the default version
            dirVersions = [x for x in self.dirVersions if x != VERSION_DEFAULT]
            # if nothing is left, the next is the start
            if len(dirVersions) == 0:
                version = VERSION_START
            # otherwise, the next is max version + 1
            else:
                version = max(dirVersions) + 1
        # if the index and directory are out of sync, find the largest in both sets
        else:
            # get the elements particular to each set -- the max of these is the next version
            indexSet = set(self.index.keys())
            diffAB = indexSet.difference(self.dirVersions)
            diffBA = self.dirVersions.difference(indexSet)
            # if diffAB is nullset, diffBA has one more member -- that is next
            if diffAB == set():
                version = list(diffBA)[0]
            # if diffBA is nullset, diffAB has one more member -- that is next
            elif diffBA == set():
                version = list(diffAB)[0]
            # otherwise find the max of both differences and return that
            else:
                indexVersion = max(diffAB)
                directoryVersion = max(diffBA)
                version = max(indexVersion, directoryVersion)

        return version

    def thisOrCurrentVersion(self, version: Optional[int]):
        if version == VERSION_DEFAULT:
            pass
        elif not isinstance(version, int) or version < 0:
            version = self.currentVersion()
        return version

    def thisOrNextVersion(self, version: Optional[int]):
        if version == VERSION_DEFAULT:
            pass
        elif not isinstance(version, int) or version < 0:
            version = self.nextVersion()
        return version

    ## VERSION COMPARISON METHODS ##

    def _isApplicableEntry(self, entry: IndexEntry, runNumber1: str):
        """
        Checks to see if an entry in the index applies to a given run id via numerical comparison.
        """

        symbol, runNumber2 = self._parseAppliesTo(entry.appliesTo)
        return self._compareRunNumbers(runNumber1, runNumber2, symbol)

    def _parseAppliesTo(self, appliesTo: str):
        return IndexEntry.parseAppliesTo(appliesTo)

    def _compareRunNumbers(self, runNumber1: str, runNumber2: str, symbol: str):
        expressions = {
            ">=": lambda x, y: x >= y,
            "<=": lambda x, y: x <= y,
            "<": lambda x, y: x < y,
            ">": lambda x, y: x > y,
            "": lambda x, y: x == y,
        }
        return expressions[symbol](int(runNumber1), int(runNumber2))

    ## PATH METHODS ##

    def indexPath(self):
        """
        Path to the index
        """
        return self.rootDirectory / f"{self.indexorType}Index.json"

    def recordPath(self, version: Optional[int] = None):
        """
        Path to a specific version of a calculation record
        """
        return self.versionPath(version) / f"{self.indexorType}Record.json"

    def parametersPath(self, version: Optional[int] = None):
        """
        Path to a specific version of calculation parameters
        """
        return self.versionPath(version) / f"{self.indexorType}Parameters.json"

    @validate_call
    def versionPath(self, version: Optional[int] = None) -> Path:
        if version is None:
            version = VERSION_START
        else:
            version = self.thisOrCurrentVersion(version)
        return self.rootDirectory / wnvf.fileVersion(version)

    def currentPath(self) -> Path:
        """
        The latest version of those in the index,
        or the version you just added to the index.
        """
        return self.versionPath(self.currentVersion())

    def latestApplicablePath(self, runNumber: str) -> Path:
        return self.versionPath(self.latestApplicableVersion(runNumber))

    ## INDEX MANIPULATION METHODS ##

    def getIndex(self) -> List[IndexEntry]:
        if self.index == {}:
            self.index = self.readIndex()

        # remove the default version, if it exists
        res = self.index.copy()
        res.pop(VERSION_DEFAULT, None)
        return list(res.values())

    def readIndex(self) -> Dict[int, IndexEntry]:
        # create the index from the index file
        indexPath: Path = self.indexPath()
        indexList: List[IndexEntry] = []
        if indexPath.exists():
            indexList = parse_file_as(List[IndexEntry], indexPath)
        return {entry.version: entry for entry in indexList}

    def writeIndex(self):
        path = self.indexPath()
        path.parent.mkdir(parents=True, exist_ok=True)
        write_model_list_pretty(self.index.values(), path)

    def addIndexEntry(self, entry: IndexEntry, version: Optional[int] = None):
        """
        If a verison is not passed, it will save at the next version.
        """
        version = self.thisOrNextVersion(version)
        entry.version = version
        self.index[entry.version] = entry
        self.writeIndex()

    def indexEntryFromRecord(self, record: Record) -> IndexEntry:
        return Record.indexEntryFromRecord(record)

    ## RECORD READ / WRITE METHODS ##

    def readRecord(self, version: Optional[int] = None) -> Record:
        """
        If no version given, defaults to current version
        """
        version = self.thisOrCurrentVersion(version)
        filePath = self.recordPath(version)
        record = None
        if filePath.exists():
            match self.indexorType:
                case IndexorType.CALIBRATION:
                    record = parse_file_as(CalibrationRecord, filePath)
                case IndexorType.NORMALIZATION:
                    record = parse_file_as(NormalizationRecord, filePath)
                case IndexorType.REDUCTION:
                    record = parse_file_as(ReductionRecord, filePath)
                case IndexorType.DEFAULT:
                    record = parse_file_as(Record, filePath)
        return record

    def writeRecord(self, record: Record, version: Optional[int] = None):
        version = self.thisOrNextVersion(version)
        record.version = version
        record.calculationParameters.version = version
        filePath = self.recordPath(version)
        filePath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(record, filePath)
        self.dirVersions.add(version)

    ## STATE PARAMETER READ / WRITE METHODS ##

    def readParameters(self, version: Optional[int] = None) -> CalculationParameters:
        """
        If no version given, defaults to current version
        """
        version = self.thisOrCurrentVersion(version)
        filePath = self.parametersPath(version)
        if filePath.exists():
            match self.indexorType:
                case IndexorType.CALIBRATION:
                    parameters = Calibration.parse_file(filePath)
                case IndexorType.NORMALIZATION:
                    parameters = Normalization.parse_file(filePath)
                case IndexorType.REDUCTION:
                    parameters = CalculationParameters.parse_file(filePath)
                case IndexorType.DEFAULT:
                    parameters = CalculationParameters.parse_file(filePath)
        else:
            raise FileNotFoundError(
                f"No {self.indexorType} calculation parameters found at {filePath} for version {version}"
            )
        return parameters

    def writeParameters(self, parameters: CalculationParameters, version: Optional[int] = None):
        version = self.thisOrNextVersion(version)
        parameters.version = version

        parametersPath = self.parametersPath(version)
        if parametersPath.exists():
            logger.warn(f"Overwriting {self.indexorType} parameters at {parametersPath}")
        else:
            parametersPath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(parameters, parametersPath)

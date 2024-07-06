import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import validate_call

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import (
    VERSION_DEFAULT,
    VERSION_DEFAULT_NAME,
    VERSION_START,
    VersionedObject,
)
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.log.logger import snapredLogger
from snapred.meta.mantid.AllowedPeakTypes import StrEnum
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

logger = snapredLogger.getLogger(__name__)


"""
    The Indexer will automatically track versions and produce the next and current versions.
    This is intended to have responsibility over one state/resolution/workflow combination,
    (e.g state abcd123efg, lite mode, calibration), and for that combination will keep tabs on
    the index and the directory tree to determine appropriate version numbers.

    All saving or loading of indices, records, or calculation parameter files should be
    handled through the appropriate Indexer.

    When saving, the Indexer will always save at the version attached to the object.
    If this version is invalid, it will throw an error and not save.

    The Indexer version list will only update when both a record and a corresponding inex entry
    have been written.
"""


class IndexerType(StrEnum):
    DEFAULT = ""
    CALIBRATION = "Calibration"
    NORMALIZATION = "Normalization"
    REDUCTION = "Reduction"


# the record type for each indexer type
RECORD_TYPE = {
    IndexerType.CALIBRATION: CalibrationRecord,
    IndexerType.NORMALIZATION: NormalizationRecord,
    IndexerType.REDUCTION: ReductionRecord,
    IndexerType.DEFAULT: Record,
}

# the params type for each indexer type
PARAMS_TYPE = {
    IndexerType.CALIBRATION: Calibration,
    IndexerType.NORMALIZATION: Normalization,
    IndexerType.REDUCTION: CalculationParameters,
    IndexerType.DEFAULT: CalculationParameters,
}


class Indexer:
    rootDirectory: Path
    index: Dict[int, IndexEntry]

    indexerType: IndexerType

    ## CONSTRUCTOR / DESTRUCTOR METHODS ##

    @validate_call
    def __init__(self, *, indexerType: IndexerType, directory: Path | str) -> None:
        self.indexerType = indexerType
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
                # Warning: order matters here:
                #   check VERSION_DEFAULT_NAME _before_ the `isdigit` check.
                if str(version) == str(VERSION_DEFAULT_NAME):
                    version = VERSION_DEFAULT
                elif version.isdigit():
                    version = int(version)
                versions.add(version)
        return versions

    def reconcileIndexToFiles(self):
        self.dirVersions = self.readDirectoryList()
        indexVersions = set(self.index.keys())

        # if a directory has no entry in the index, warn
        missingEntries = self.dirVersions.difference(indexVersions)
        if len(missingEntries) > 0:
            logger.warn(f"The following versions are expected, but missing from the index: {missingEntries}")

        # if an entry in the index has no directory, throw an error
        missingRecords = indexVersions.difference(self.dirVersions)
        if len(missingRecords) > 0:
            indexVersions = indexVersions - missingRecords
            # This exception would otherwise always be thrown during a test-teardown sequence,
            #   which spams the test logs, even when nothing is wrong.
            if "pytest" not in sys.modules:
                raise FileNotFoundError(
                    f"The following records were expected, but not available on disk: {missingRecords}"
                )
            else:
                logger.warn(f"The following records were expected, but not available on disk: {missingRecords}")

        # take the set of versions common to both
        commonVersions = self.dirVersions & indexVersions
        self.dirVersions = commonVersions
        self.index = {version: self.index[version] for version in commonVersions}

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
        The largest version found by the Indexer.
        """
        version = None
        overlap = set.union(set(self.index.keys()), self.dirVersions)
        if len(overlap) == 0:
            version = None
        elif len(overlap) == 1:
            version = list(overlap)[0]
        else:
            versions = [v for v in overlap if isinstance(v, int)]
            version = max(versions)
        return version

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

    @validate_call
    def thisOrCurrentVersion(self, version: Optional[int]):
        if self.isValidVersion(version):
            return version
        else:
            return self.currentVersion()

    @validate_call
    def thisOrNextVersion(self, version: Optional[int]):
        if self.isValidVersion(version):
            return version
        else:
            return self.nextVersion()

    @validate_call
    def thisOrLatestApplicableVersion(self, runNumber: str, version: Optional[int]):
        if self.isValidVersion(version) and self._isApplicableEntry(self.index[version], runNumber):
            return version
        else:
            return self.latestApplicableVersion(runNumber)

    def isValidVersion(self, version):
        try:
            VersionedObject.parseVersion(version, exclude_none=True)
            return True
        except ValueError:
            return False

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
        return self.rootDirectory / f"{self.indexerType}Index.json"

    def recordPath(self, version: Optional[int] = None):
        """
        Path to a specific version of a calculation record
        """
        return self.versionPath(version) / f"{self.indexerType}Record.json"

    def parametersPath(self, version: Optional[int] = None):
        """
        Path to a specific version of calculation parameters
        """
        return self.versionPath(version) / f"{self.indexerType}Parameters.json"

    @validate_call
    def versionPath(self, version: Optional[int] = None) -> Path:
        if version is None:
            version = VERSION_START
        else:
            version = self.thisOrCurrentVersion(version)
        return self.rootDirectory / wnvf.pathVersion(version)

    def currentPath(self) -> Path:
        """
        The latest version of those in the index,
        or the version you just added to the index.
        """
        return self.versionPath(self.currentVersion())

    def latestApplicablePath(self, runNumber: str) -> Path:
        return self.versionPath(self.latestApplicableVersion(runNumber))

    ## INDEX MANIPULATION METHODS ##

    def createIndexEntry(self, *, version, **other_arguments):
        return IndexEntry(
            version=self.thisOrNextVersion(version),
            **other_arguments,
        )

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

    def addIndexEntry(self, entry: IndexEntry):
        """
        Will save at the version on the index entry.
        If the version is invalid, will throw an error and refuse to save.
        """
        if not self.isValidVersion(entry.version):
            raise RuntimeError(f"Invalid version {entry.version} on index entry.  Save failed.")

        self.index[entry.version] = entry
        self.writeIndex()

    ## RECORD READ / WRITE METHODS ##

    def createRecord(self, *, version, **other_arguments):
        record = RECORD_TYPE[self.indexerType](
            version=self.thisOrNextVersion(version),
            **other_arguments,
        )
        record.calculationParameters.version = record.version
        return record

    def readRecord(self, version: Optional[int] = None) -> Record:
        """
        If no version given, defaults to current version
        """
        version = self.thisOrCurrentVersion(version)
        filePath = self.recordPath(version)
        record = None
        if filePath.exists():
            record = parse_file_as(RECORD_TYPE[self.indexerType], filePath)
        return record

    def writeRecord(self, record: Record):
        """
        Will save at the version on the record.
        If the version is invalid, will throw an error and refuse to save.
        """
        if not self.isValidVersion(record.version):
            raise RuntimeError(f"Invalid version {record.version} on record.  Save failed.")

        filePath = self.recordPath(record.version)
        filePath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(record, filePath)
        self.dirVersions.add(record.version)

    ## STATE PARAMETER READ / WRITE METHODS ##

    def createParameters(self, *, version, **other_arguments) -> CalculationParameters:
        return PARAMS_TYPE[self.indexerType](
            version=self.thisOrNextVersion(version),
            **other_arguments,
        )

    def readParameters(self, version: Optional[int] = None) -> CalculationParameters:
        """
        If no version given, defaults to current version
        """
        version = self.thisOrCurrentVersion(version)
        filePath = self.parametersPath(version)
        parameters = None
        if filePath.exists():
            parameters = parse_file_as(PARAMS_TYPE[self.indexerType], filePath)
        else:
            raise FileNotFoundError(
                f"No {self.indexerType} calculation parameters found at {filePath} for version {version}"
            )
        return parameters

    def writeParameters(self, parameters: CalculationParameters):
        """
        Will save at the version on the calculation parameters.
        If the version is invalid, will throw an error and refuse to save.
        """
        if not self.isValidVersion(parameters.version):
            raise RuntimeError(f"Invalid version {parameters.version} on calculation parameters.  Save failed.")

        parametersPath = self.parametersPath(parameters.version)
        if parametersPath.exists():
            logger.warn(f"Overwriting {self.indexerType} parameters at {parametersPath}")
        else:
            parametersPath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(parameters, parametersPath)
        self.dirVersions.add(parameters.version)

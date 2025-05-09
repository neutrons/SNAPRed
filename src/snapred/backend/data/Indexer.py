import os
import sys
from pathlib import Path
from typing import Dict, List, Type, TypeVar

from pydantic import validate_call

from snapred.backend.dao import InstrumentConfig
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationDefaultRecord, CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_START, Version, VersionedObject, VersionState
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Enum import StrEnum
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty, write_model_pretty

logger = snapredLogger.getLogger(__name__)

T = TypeVar("T", bound=VersionedObject)

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
    INSTRUMENT_PARAMETER = "InstrumentParameter"


# the record type for each indexer type
RECORD_TYPE = {
    IndexerType.CALIBRATION: CalibrationRecord,
    IndexerType.NORMALIZATION: NormalizationRecord,
    IndexerType.REDUCTION: ReductionRecord,
    IndexerType.INSTRUMENT_PARAMETER: None,
    IndexerType.DEFAULT: Record,
}

DEFAULT_RECORD_TYPE = {
    IndexerType.CALIBRATION: CalibrationDefaultRecord,
}

# the params type for each indexer type
PARAMS_TYPE = {
    IndexerType.CALIBRATION: Calibration,
    IndexerType.NORMALIZATION: Normalization,
    IndexerType.REDUCTION: CalculationParameters,
    IndexerType.DEFAULT: CalculationParameters,
}

FRIENDLY_NAME_MAPPING = {
    Calibration.__name__: "CalibrationParameters",
    CalibrationDefaultRecord.__name__: "CalibrationRecord",
    CalibrationRecord.__name__: "CalibrationRecord",
    Normalization.__name__: "NormalizationParameters",
    NormalizationRecord.__name__: "NormalizationRecord",
    ReductionRecord.__name__: "ReductionRecord",
    InstrumentConfig.__name__: "SNAPInstPrm",
    CalculationParameters.__name__: "CalculationParameters",
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
                #   check VersionState.DEFAULT _before_ the `isdigit` check.
                if version in [VersionState.DEFAULT, self.defaultVersion()]:
                    version = self.defaultVersion()
                elif version.isdigit():
                    version = int(version)
                else:
                    logger.warning(f"Invalid version in directory: {version}")
                    continue
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
                logger.warning(
                    (
                        f"The following records were expected, but not available on disk: {missingRecords}",
                        "\n Please contact your IS or CIS about these missing records.",
                    )
                )

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
        return VERSION_START()

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
            if self.defaultVersion() in self.index:
                relevantEntries.remove(self.index[self.defaultVersion()])
            version = relevantEntries[-1].version
        return version

    def nextVersion(self) -> int:
        """
        A new version number to use for saving calibration records.
        """

        # if the index and directories are in sync, the next version is one past them
        if set(self.index.keys()) != self.dirVersions:
            self.reconcileIndexToFiles()

        if self.currentVersion() is None:
            return self.defaultVersion()
        else:
            return self.currentVersion() + 1

    def validateVersion(self, version):
        try:
            VersionedObject(version=version)
            return True
        except ValueError:
            # This error would only ever result from a software bug.
            # Saving/Loading/Refering to erroneous "current" versions just serves to obfuscate the error.
            raise ValueError(
                (
                    f"The indexer has encountered an invalid version: {version}.",
                    "This is a software error.  Please report this to your IS or CIS",
                    "so it may be patched.",
                )
            )

    ## VERSION COMPARISON METHODS ##

    def _isApplicableEntry(self, entry: IndexEntry, runNumber1: str):
        """
        Checks to see if an entry in the index applies to a given run id via numerical comparison.
        """
        isApplicable = True
        conditionals = self._parseAppliesTo(entry.appliesTo)
        for symbol, runNumber2 in conditionals:
            isApplicable = isApplicable and self._compareRunNumbers(runNumber1, runNumber2, symbol)
        return isApplicable

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

    def recordPath(self, version: int):
        """
        Path to a specific version of a calculation record
        """
        return self.versionPath(version) / f"{self.indexerType}Record.json"

    def parametersPath(self, version: int):
        """
        Path to a specific version of calculation parameters
        """
        return self.versionPath(version) / f"{self.indexerType}Parameters.json"

    def versionPath(self, version: int) -> Path:
        self.validateVersion(version)
        return self.rootDirectory / wnvf.pathVersion(version)

    def currentPath(self) -> Path:
        """
        The latest version of those in the index,
        or the version you just added to the index.
        """
        return self.versionPath(self.currentVersion())

    def getLatestApplicablePath(self, runNumber: str) -> Path:
        return self.versionPath(self.latestApplicableVersion(runNumber))

    ## INDEX MANIPULATION METHODS ##

    def createIndexEntry(self, *, version, **other_arguments):
        return IndexEntry(
            version=self._flattenVersion(version),
            **other_arguments,
        )

    def getIndex(self) -> List[IndexEntry]:
        if self.index == {}:
            self.index = self.readIndex()

        # remove the default version, if it exists
        res = self.index.copy()
        res.pop(self.defaultVersion(), None)
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
        entry.version = self._flattenVersion(entry.version)
        self.index[entry.version] = entry
        self.writeIndex()

    ## RECORD READ / WRITE METHODS ##

    def createRecord(self, *, version, **other_arguments):
        record = RECORD_TYPE[self.indexerType](
            version=self._flattenVersion(version),
            **other_arguments,
        )
        record.calculationParameters.version = record.version
        return record

    def _determineRecordType(self, version: int):
        recordType = None
        if version == self.defaultVersion():
            recordType = DEFAULT_RECORD_TYPE.get(self.indexerType, None)
        if recordType is None:
            recordType = RECORD_TYPE[self.indexerType]
        return recordType

    def readRecord(self, version: int) -> Record:
        """
        If no version given, defaults to current version
        """
        recordType = self._determineRecordType(version)
        return self.readVersionedObject(recordType, version)

    def _flattenVersion(self, version: Version):
        """
        Converts a version to an int.
        This should only ever be used on write,
        converting VersionState to a version that doesnt exist.
        i.e. next, or default before state initialization.
        """
        flattenedVersion = None
        if version == VersionState.DEFAULT:
            flattenedVersion = self.defaultVersion()
        elif version == VersionState.NEXT:
            flattenedVersion = self.nextVersion()
        elif isinstance(version, int):
            flattenedVersion = version
        else:
            acceptableVersionShorthands = [VersionState.DEFAULT, VersionState.NEXT]
            raise ValueError(f"Version must be an int or {[acceptableVersionShorthands]}, not {version}")

        if flattenedVersion is None:
            raise ValueError(
                f"No available versions found during lookup using: "
                f"v={version}, index={self.index}, dir={self.dirVersions}"
            )
        return flattenedVersion

    def versionExists(self, version: Version):
        return self._flattenVersion(version) in self.index

    def writeNewVersion(self, record: Record, entry: IndexEntry):
        """
        Coupled write of a record and an index entry.
        As required for new records.
        """
        if self.versionExists(record.version):
            raise ValueError(f"Version {record.version} already exists in index, please write a new version.")

        if entry.appliesTo is None:
            entry.appliesTo = ">=" + record.runNumber

        self.addIndexEntry(entry)
        # make sure they flatten to the same value.
        record.version = entry.version
        record.calculationParameters.version = entry.version
        self.writeRecord(record)

    def versionedObjectPath(self, type_: Type[T], version: Version):
        """
        Path to a specific version of a calculation record
        """
        fileName = FRIENDLY_NAME_MAPPING.get(type_.__name__, type_.__name__)
        return self.versionPath(version) / f"{fileName}.json"

    def writeNewVersionedObject(self, obj: VersionedObject, entry: IndexEntry):
        """
        Coupled write of parameters and an index entry.
        As required for new parameters.
        """
        if self.versionExists(obj.version):
            raise ValueError(f"Version {obj.version} already exists in index, please write a new version.")

        self.addIndexEntry(entry)
        # make sure they flatten to the same value.
        obj.version = entry.version
        self.writeVersionedObject(obj)

    def writeVersionedObject(self, obj: VersionedObject):
        """
        Will save at the version on the object.
        If the version is invalid, will throw an error and refuse to save.
        """
        obj.version = self._flattenVersion(obj.version)

        if not self.versionExists(obj.version):
            raise ValueError(f"Version {obj.version} not found in index, please write an index entry first.")

        filePath = self.versionedObjectPath(type(obj), obj.version)
        filePath.parent.mkdir(parents=True, exist_ok=True)

        write_model_pretty(obj, filePath)

        self.dirVersions.add(obj.version)

    def readVersionedObject(self, type_: Type[T], version: Version) -> VersionedObject:
        """
        If no version given, defaults to current version
        """
        filePath = self.versionedObjectPath(type_, version)
        obj = None
        if filePath.exists():
            obj = parse_file_as(type_, filePath)
        else:
            raise FileNotFoundError(f"No {type_.__name__} found at {filePath} for version {version}")
        return obj

    def writeRecord(self, record: Record):
        """
        Will save at the version on the record.
        If the version is invalid, will throw an error and refuse to save.
        """
        self.writeVersionedObject(record)

    ## STATE PARAMETER READ / WRITE METHODS ##

    def createParameters(self, *, version, **other_arguments) -> CalculationParameters:
        return PARAMS_TYPE[self.indexerType](
            version=self._flattenVersion(version),
            **other_arguments,
        )

    def readParameters(self, version: Version) -> CalculationParameters:
        """
        If no version given, defaults to current version
        """
        return self.readVersionedObject(PARAMS_TYPE[self.indexerType], version)

    def writeParameters(self, parameters: CalculationParameters):
        """
        Will save at the version on the calculation parameters.
        If the version is invalid, will throw an error and refuse to save.
        """
        self.writeVersionedObject(parameters)

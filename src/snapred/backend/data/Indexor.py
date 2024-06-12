import os
from pathlib import Path
from typing import Dict, List, Optional

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry, Version
from snapred.backend.dao.indexing.Record import Nonrecord, Record
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
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

    # starting version numbers
    VERSION_START = Config["version.error"]
    VERSION_DEFAULT = Config["version.error"]
    UNINITIALIZED = Config["version.error"]

    ## CONSTRUCTOR / DESTRUCTOR METHODS ##

    def __init__(self, *, indexorType: str, directory: Path | str) -> None:
        self.indexorType = indexorType
        self.VERSION_START = Config[f"version.{self.indexorType.lower()}.start"]
        if indexorType == IndexorType.CALIBRATION:
            self.VERSION_DEFAULT = Config[f"version.{self.indexorType.lower()}.default"]
        self.rootDirectory = Path(directory)
        self.index = self.readIndex()
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
                version = int(str(fname).split("_")[-1])
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

    def allVersions(self) -> List[Version]:
        return list(self.index.keys())

    def defaultVersion(self) -> Version:
        """
        The version number to use for default states.
        """
        return self.VERSION_DEFAULT

    def currentVersion(self) -> Version:
        """
        The largest version in the index.
        """
        if len(self.index) == 0:
            return self.UNINITIALIZED
        else:
            return max(self.index.keys())

    def latestApplicableVersion(self, runNumber: str) -> Version:
        """
        The most recent version in time, which is applicable to the run number.
        """
        # sort by timestamp
        entries = list(self.index.values())
        entries.sort(key=lambda x: x.timestamp)
        # filter for latest applicable
        relevantEntries = list(filter(lambda x: self._isApplicableEntry(x, runNumber), entries))
        if len(relevantEntries) < 1:
            version = self.UNINITIALIZED
        else:
            version = relevantEntries[-1].version
        return version

    def nextVersion(self) -> Version:
        """
        A new version number to use for saving calibration records.
        """

        version = self.UNINITIALIZED

        # if nothing is in the index, the new version is starting version
        if len(self.index) == 0:
            return self.VERSION_START

        # determine the next version by comparing index to directory tree
        else:
            dirVersions = self.readDirectoryList()
            maxIndexVersion = max(self.index.keys(), default=self.VERSION_START - 1)
            maxDirVersion = max(dirVersions, default=self.VERSION_START - 1)

            # if indices and records are paired
            # then the next version is the current + 1
            if maxIndexVersion == maxDirVersion:
                version = maxIndexVersion + 1

            # otherwise, use the most updated version
            else:
                return max(maxIndexVersion, maxDirVersion)

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

    def recordPath(self, version: Version):
        """
        Path to a specific version of a calculation record
        """
        return self.versionPath(version) / f"{self.indexorType}Record.json"

    def parametersPath(self, version: Version):
        """
        Path to a specific version of calculation parameters
        """
        return self.versionPath(version) / f"{self.indexorType}Parameters.json"

    def versionPath(self, version: Version) -> Path:
        if version is self.UNINITIALIZED:
            version = self.VERSION_START
        elif version == "*":
            version = max(self.index.keys(), default=self.VERSION_START)
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
        return list(self.index.values())

    def readIndex(self) -> Dict[Version, IndexEntry]:
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

    def addIndexEntry(self, entry: IndexEntry, version: Optional[Version] = None):
        """
        If a verison is not passed, it will save at the next version.
        """
        if not isinstance(version, int):
            version = self.nextVersion()
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
        if not isinstance(version, int):
            version = self.currentVersion()
        filePath = self.recordPath(version)
        if filePath.exists():
            match self.indexorType:
                case IndexorType.CALIBRATION:
                    record = CalibrationRecord.parse_file(filePath)
                case IndexorType.NORMALIZATION:
                    record = NormalizationRecord.parse_file(filePath)
                case IndexorType.REDUCTION:
                    record = ReductionRecord.parse_file(filePath)
                case IndexorType.DEFAULT:
                    record = Record.parse_file(filePath)
            # NOTE the calculation parameters are absent from any saved records
            # read the calculation parameters separately from their own file
            record.calculationParameters = self.readParameters(version)
        else:
            record = Nonrecord
        return record

    def writeRecord(self, record: Record, version: Optional[int] = None):
        if not isinstance(version, int):
            version = self.nextVersion()
        record.version = version
        filePath = self.recordPath(version)
        filePath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(record, filePath)
        # NOTE calculation parameters are excluded from serialization
        # write the calculation parameters to a separate file
        self.writeParameters(record.calculationParameters, version)

    ## STATE PARAMETER READ / WRITE METHODS ##

    def readParameters(self, version: Optional[int] = None) -> CalculationParameters:
        """
        If no version given, defaults to current version
        """
        if not isinstance(version, int):
            version = self.currentVersion()
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
        if not isinstance(version, int):
            version = self.nextVersion()
        parameters.version = version

        parametersPath = self.parametersPath(version)
        if parametersPath.exists():
            logger.warn(f"Overwriting {self.indexorType} parameters at {parametersPath}")
        else:
            parametersPath.parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(parameters, parametersPath)

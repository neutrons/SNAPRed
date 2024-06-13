# ruff: noqa: ARG005 ARG002
import json
import os
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple, Union

from mantid.simpleapi import mtd
from pydantic import validate_call
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.state import (
    GroupingMap,
)
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton

Version = Union[int, Literal["*"]]
logger = snapredLogger.getLogger(__name__)


@Singleton
class WhateversInTheFridge(LocalDataService):
    """
    Yeah, it'd be nice to use the LocalDataService and get all this...
    But that's really complicated, and we don't have time.
    Just grab whatever's in the fridge.

    Can mock out the LocalDataService for testing
    """

    iptsCache: Dict[Tuple[str, str], Any] = {}

    def __init__(self) -> None:
        self.verifyPaths = False
        self.instrumentConfig = self.readInstrumentConfig()
        self.mantidSnapper = MantidSnapper(None, "Utensils")
        self.latestVersion = Config["instrument.startingVersionNumber"]

    ### MISCELLANEOUS ###
    def readCifFilePath(self, sampleId: str):
        samplePath: str = Resource.getPath("inputs/calibrantSamples/")
        fileName: str = Resource.getPath("inputs/calibrantSamples/Silicon_NIST_640D_001.json")
        filePath = os.path.join(samplePath, fileName)
        if not os.path.exists(filePath):
            raise ValueError(f"the file '{filePath}' does not exist")
        with open(filePath, "r") as f:
            calibrantSampleDict = json.load(f)
        filePath = Path(calibrantSampleDict["crystallography"]["cifFile"])
        # Allow relative paths:
        if not filePath.is_absolute():
            filePath = Path(Config["samples.home"]).joinpath(filePath)
        return str(filePath)

    ### FILE METHODS ###

    def fileExists(self, filepath):
        if filepath == "does/not/exist":
            return False
        elif filepath == "file/does/exist":
            return True
        else:
            return os.path.exists(filepath)

    def getIPTS(self, runNumber: str, instrumentName: str = Config["instrument.name"]) -> str:
        key = (runNumber, instrumentName)
        if key not in self.iptsCache:
            self.iptsCache[key] = mtd.unique_name(prefix=f"{runNumber}_")
        return str(self.iptsCache[key])

    @ExceptionHandler(StateValidationException)
    def _generateStateId(self, runId: str) -> Tuple[str, str]:
        return "outpus/2kfxjiqm", "some gibberish"

    def checkCalibrationFileExists(self, runId: str):
        if runId.isdigit():
            return True
        else:
            return False

    ### CALIBRATION METHODS ###

    @validate_call
    def readCalibrationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        version = version if version is not None else self.latestVersion
        record = CalibrationRecord.construct(
            runNumber=runId,
            useLiteMode=useLiteMode,
            version=version,
        )
        return record

    @validate_call
    def _getVersionFromCalibrationIndex(self, runId: str, useLiteMode: bool) -> int:
        return self.latestVersion

    @validate_call
    def _getLatestCalibrationVersionNumber(self, stateId: str, useLiteMode: bool) -> int:
        return self.latestVersion

    ### NORMALIZATION METHODS ###

    @validate_call
    def readNormalizationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        version = version if version is not None else self.latestVersion
        record = NormalizationRecord.construct(
            runNumber=runId,
            useLiteMode=useLiteMode,
            version=version,
        )
        return record

    @validate_call
    def _getVersionFromNormalizationIndex(self, runId: str, useLiteMode: bool) -> int:
        return self.latestVersion

    @validate_call
    def _getLatestNormalizationVersionNumber(self, stateId: str, useLiteMode: bool) -> int:
        return self.latestVersion

    ### REDUCTION METHODS ###

    def writeReductionRecord(self, record: ReductionRecord, version: Optional[int]) -> ReductionRecord:
        record.runNumbers.append("writeReductionRecord")
        return record

    def writeReductionData(self, record: ReductionRecord) -> ReductionRecord:
        record.runNumbers.append("writeReductionData")
        return record

    ### GROUPING MAP METHODS ###

    def _groupingMapPath(self, stateId) -> Path:
        return Resource.getPath("inputs/testInstrument/groupingMap.json")

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        thismap = self._readDefaultGroupingMap()
        thismap.stateId = stateId
        return thismap

    def _readDefaultGroupingMap(self) -> GroupingMap:
        thismap = GroupingMap.parse_file(self._groupingMapPath("fakeStateID"))
        thismap = GroupingMap.construct(
            stateId=thismap.stateId,
            nativeFocusGroups=thismap.nativeFocusGroups,
            liteFocusGroups=thismap.liteFocusGroups,
            _nativeMap={x.name: x for x in thismap.nativeFocusGroups},
            _liteMap={x.name: x for x in thismap.liteFocusGroups},
        )
        return thismap

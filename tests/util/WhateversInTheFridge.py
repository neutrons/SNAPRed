# ruff: noqa: ARG005 ARG002
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from mantid.simpleapi import CreateSingleValuedWorkspace, mtd
from pydantic import validate_call
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.state import GroupingMap
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config, Resource
from snapred.meta.decorators.ExceptionHandler import ExceptionHandler
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from util.dao import DAOFactory

logger = snapredLogger.getLogger(__name__)


@Singleton
class WhateversInTheFridge(LocalDataService):
    """
    Yeah, it'd be nice to go to the LocalDataService to get all this...
    But that's really complicated, and we don't have time.
    Just grab whatever's in the fridge.

    Can mock out the LocalDataService for testing.
    Only mocks out the factory methods; for export methods, use state_root_redirect
    """

    iptsCache: Dict[Tuple[str, str], Any] = {}

    def __init__(self) -> None:
        self.verifyPaths = False
        self.instrumentConfig = self.readInstrumentConfig()
        self.mantidSnapper = MantidSnapper(None, "Utensils")
        self.latestVersion = Config["version.start"]

    ### MISCELLANEOUS ###

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
        stateId = DAOFactory.magical_state_id.copy()
        return stateId.hex, stateId.decodedKey

    ### CALIBRATION METHODS ###

    def calculationParameters_with_stateId(self, stateId: str):
        return CalculationParameters.model_construct(
            instrumentState=InstrumentState.model_construct(
                id=ObjectSHA.model_construct(
                    hex=stateId,
                    decodedKey="gibberish",
                )
            )
        )

    @validate_call
    def readCalibrationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        version = version if version is not None else self.latestVersion
        record = CalibrationRecord.model_construct(
            runNumber=runId,
            useLiteMode=useLiteMode,
            version=version,
            calculationParameters=self.calculationParameters_with_stateId("0xdeadbeef"),
        )
        return record

    ### NORMALIZATION METHODS ###

    @validate_call
    def readNormalizationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        version = version if version is not None else self.latestVersion
        record = NormalizationRecord.model_construct(
            runNumber=runId,
            useLiteMode=useLiteMode,
            version=version,
            calculationParameters=self.calculationParameters_with_stateId("0xdeadbeef"),
        )
        return record

    ### REDUCTION METHODS ###

    def readReductionRecord(self, runNumber: str, useLiteMode: bool, timestamp: float):
        wsName = (
            wng.reductionOutput().unit(wng.Units.DSP).group("bank").runNumber(runNumber).timestamp(timestamp).build()
        )
        CreateSingleValuedWorkspace(OutputWorkspace=wsName)
        return ReductionRecord.model_construct(
            runNumber=runNumber,
            useLiteMode=useLiteMode,
            timestamp=timestamp,
            workspaceNames=[wsName],
            calibration=self.readCalibrationRecord(runNumber, useLiteMode, 1),
            normalization=self.readNormalizationRecord(runNumber, useLiteMode, 1),
        )

    ### READ / WRITE STATE METHODS ###

    def readDetectorState(self, runId: str):
        return DetectorState.construct(wav=1.0)

    def checkCalibrationFileExists(self, runId: str):
        if runId.isdigit():
            return True
        else:
            return False

    ### CALIBRANT SAMPLE METHODS ###

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

    ### GROUPING MAP METHODS ###

    def _groupingMapPath(self, stateId) -> Path:
        return Path(Resource.getPath("inputs/testInstrument/groupingMap.json"))

    def _readGroupingMap(self, stateId: str) -> GroupingMap:
        return DAOFactory.groupingMap_POP(stateId)

    def _readDefaultGroupingMap(self) -> GroupingMap:
        return DAOFactory.groupingMap_POP(stateId=DAOFactory.nonsense_state_id)

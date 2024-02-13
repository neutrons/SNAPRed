import json
import os
import shutil
import socket
import tempfile
import unittest.mock as mock
from pathlib import Path
from typing import List

import pytest
from pydantic import parse_raw_as
from pydantic.error_wrappers import ValidationError
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples

# NOTE this is necessary to prevent mocking out needed functions
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.meta.Config import Config, Resource
from snapred.meta.redantic import write_model_pretty

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

# Mock out of scope modules before importing DataExportService
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock(), "h5py": mock.Mock()}):
    from snapred.backend.dao import StateConfig
    from snapred.backend.dao.calibration.Calibration import Calibration  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ingredients import ReductionIngredients  # noqa: E402
    from snapred.backend.dao.normalization.Normalization import Normalization  # noqa: E402
    from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry  # noqa: E402
    from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord  # noqa: E402
    from snapred.backend.dao.state.FocusGroup import FocusGroup
    from snapred.backend.dao.state.GroupingMap import GroupingMap
    from snapred.backend.dao.state.InstrumentState import InstrumentState
    from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402

    reductionIngredients = None
    with Resource.open("inputs/calibration/ReductionIngredients.json", "r") as file:
        reductionIngredients = parse_raw_as(ReductionIngredients, file.read())

    def _readInstrumentParameters():
        instrumentParmaeters = None
        with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
            instrumentParmaeters = json.loads(file.read())
        return instrumentParmaeters

    def test_readInstrumentConfig():
        localDataService = LocalDataService()
        localDataService._readInstrumentParameters = _readInstrumentParameters
        actual = localDataService.readInstrumentConfig()
        assert actual is not None
        assert actual.version == "1.4"
        assert actual.name == "SNAP"

    def test_readInstrumentParameters():
        localDataService = LocalDataService()
        localDataService.instrumentConfigPath = Resource.getPath("inputs/SNAPInstPrm.json")
        actual = localDataService._readInstrumentParameters()
        assert actual is not None
        assert actual["version"] == 1.4
        assert actual["name"] == "SNAP"

    def getMockInstrumentConfig():
        instrumentConfig = mock.Mock()
        instrumentConfig.calibrationDirectory = Path("test")
        instrumentConfig.sharedDirectory = "test"
        instrumentConfig.reducedDataDirectory = "test"
        instrumentConfig.pixelGroupingDirectory = "test"
        instrumentConfig.delTOverT = 1
        instrumentConfig.nexusDirectory = "test"
        instrumentConfig.nexusFileExtension = "test"
        return instrumentConfig

    def test_readStateConfig():
        localDataService = LocalDataService()
        localDataService.readGroupingFiles = mock.Mock(return_value=["/grouping1.json"])
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        localDataService._groupingMapPath = mock.Mock()
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json")
        )
        stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
        localDataService._readGroupingMap = mock.Mock()
        localDataService._readGroupingMap.return_value = stateGroupingMap

        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig("57514")
        assert actual is not None
        assert actual.stateId == "ab8704b0bc2a2342"

    def test_readStateConfig_attaches_grouping_map():
        # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
        localDataService = LocalDataService()
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        localDataService._groupingMapPath = mock.Mock()
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json")
        )

        stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
        localDataService._readGroupingMap = mock.Mock()
        localDataService._readGroupingMap.return_value = stateGroupingMap

        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig("57514")
        assert actual.groupingMap == stateGroupingMap

    def test_readStateConfig_sets_grouping_map_stateid():
        # Test that the first time a `StateConfig` is initialized,
        #   the 'stateId' of the `StateConfig.groupingMap` is set to match that of the `StateConfig`.
        localDataService = LocalDataService()
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        # `GroupingMap` JSON file doesn't exist => read default `GroupingMap`
        localDataService._groupingMapPath = mock.Mock()
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/does_not_exist.json")
        )

        defaultGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
        localDataService._readDefaultGroupingMap = mock.Mock()
        localDataService._readDefaultGroupingMap.return_value = defaultGroupingMap

        localDataService._writeGroupingMap = mock.Mock()

        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig("57514")
        assert actual.groupingMap.stateId == actual.stateId

    def test_readStateConfig_writes_grouping_map():
        # Test that the first time a `StateConfig` is initialized,
        #   the `StateConfig.groupingMap` is written to the <state root> directory.
        localDataService = LocalDataService()
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService._groupingMapPath = mock.Mock()
            groupingMapOutputPath = Path(f"{tempdir}/groupingMap.json")
            assert not groupingMapOutputPath.exists()
            localDataService._groupingMapPath.return_value = groupingMapOutputPath

            defaultGroupingMap = GroupingMap.parse_file(
                Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
            )
            localDataService._readDefaultGroupingMap = mock.Mock()
            localDataService._readDefaultGroupingMap.return_value = defaultGroupingMap

            localDataService.instrumentConfig = getMockInstrumentConfig()

            actual = localDataService.readStateConfig("57514")
            assert groupingMapOutputPath.exists()
            with open(groupingMapOutputPath, "r") as file:
                groupingMap = parse_raw_as(GroupingMap, file.read())
            assert groupingMap.stateId == actual.stateId

    def test_readStateConfig_invalid_grouping_map():
        # Test that the attached grouping-schema map's 'stateId' is checked.
        with pytest.raises(  # noqa: PT012
            RuntimeError,
            match="the state configuration's grouping map must have the same 'stateId' as the configuration",
        ):
            localDataService = LocalDataService()
            localDataService._readDiffractionCalibrant = mock.Mock()
            localDataService._readDiffractionCalibrant.return_value = (
                reductionIngredients.reductionState.stateConfig.diffractionCalibrant
            )
            localDataService._readNormalizationCalibrant = mock.Mock()
            localDataService._readNormalizationCalibrant.return_value = (
                reductionIngredients.reductionState.stateConfig.normalizationCalibrant
            )
            localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
            localDataService._readPVFile = mock.Mock()
            fileMock = mock.Mock()
            localDataService._readPVFile.return_value = fileMock
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService.readCalibrationState = mock.Mock()
            localDataService.readCalibrationState.return_value = Calibration.parse_file(
                Resource.getPath("inputs/calibration/CalibrationParameters.json")
            )

            localDataService._groupingMapPath = mock.Mock()
            # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
            localDataService._groupingMapPath.return_value = Path(
                Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json")
            )
            stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/defaultGroupingMap.json"))
            localDataService._readGroupingMap = mock.Mock()
            localDataService._readGroupingMap.return_value = stateGroupingMap

            localDataService.instrumentConfig = getMockInstrumentConfig()

            localDataService.readStateConfig("57514")

    def test_readStateConfig_grouping_map_JSON_file_not_found():
        # Test that a grouping-schema map is required during the `readStateConfig` method:
        #   * note that a non-existing 'groupingMap.json' at the <state root>
        #     triggers a fallback to 'defaultGroupingMap.json'
        #     at Config['instrument.calibration.powder.grouping.home'].
        localDataService = LocalDataService()
        nonExistentPath = Path(Resource.getPath("inputs/pixel_grouping/does_not_exist.json"))
        localDataService._groupingMapPath = mock.Mock()
        localDataService._groupingMapPath.return_value = nonExistentPath
        localDataService._defaultGroupingMapPath = mock.Mock()
        localDataService._defaultGroupingMapPath.return_value = nonExistentPath
        with pytest.raises(  # noqa: PT012
            FileNotFoundError,
            match=f'required default grouping-schema map "{nonExistentPath}" does not exist',
        ):
            localDataService._readDiffractionCalibrant = mock.Mock()
            localDataService._readDiffractionCalibrant.return_value = (
                reductionIngredients.reductionState.stateConfig.diffractionCalibrant
            )
            localDataService._readNormalizationCalibrant = mock.Mock()
            localDataService._readNormalizationCalibrant.return_value = (
                reductionIngredients.reductionState.stateConfig.normalizationCalibrant
            )
            localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
            localDataService._readPVFile = mock.Mock()
            fileMock = mock.Mock()
            localDataService._readPVFile.return_value = fileMock
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService.readCalibrationState = mock.Mock()
            localDataService.readCalibrationState.return_value = Calibration.parse_file(
                Resource.getPath("inputs/calibration/CalibrationParameters.json")
            )
            localDataService.instrumentConfig = getMockInstrumentConfig()
            localDataService.readStateConfig("57514")

    def test_write_model_pretty_StateConfig_excludes_grouping_map():
        # At present there is no `writeStateConfig` method, and there is no `readStateConfig` that doesn't
        #   actually build up the `StateConfig` from its components.
        # This test verifies that `GroupingMap` is excluded from any future `StateConfig` JSON serialization.
        localDataService = LocalDataService()
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )

        localDataService._groupingMapPath = mock.Mock()
        localDataService._groupingMapPath.return_value = Path(
            Resource.getPath("inputs/pixel_grouping/groupingMap.json")
        )
        stateGroupingMap = GroupingMap.parse_file(Resource.getPath("inputs/pixel_grouping/groupingMap.json"))
        localDataService._readGroupingMap = mock.Mock()
        localDataService._readGroupingMap.return_value = stateGroupingMap

        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig("57514")
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            stateConfigPath = Path(tempdir) / "stateConfig.json"
            write_model_pretty(actual, stateConfigPath)
            # read it back in:
            stateConfig = None
            with open(stateConfigPath, "r") as file:
                stateConfig = parse_raw_as(StateConfig, file.read())
            assert stateConfig.groupingMap is None

    def test_readRunConfig():
        localDataService = LocalDataService()
        localDataService._readRunConfig = mock.Mock()
        localDataService._readRunConfig.return_value = reductionIngredients.runConfig
        actual = localDataService.readRunConfig(mock.Mock())
        assert actual is not None
        assert actual.runNumber == "57514"

    def test__readRunConfig():
        localDataService = LocalDataService()
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readRunConfig("57514")
        assert actual is not None
        assert actual.runNumber == "57514"

    def test_readPVFile():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = getMockInstrumentConfig()
        localDataService._constructPVFilePath = mock.Mock()
        localDataService._constructPVFilePath.return_value = Resource.getPath("./")
        actual = localDataService._readPVFile(mock.Mock())
        assert actual is not None

    def test__generateStateId():
        localDataService = LocalDataService()
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        fileMock.get.side_effect = [[0.1], [0.1], [0.1], [0.1], [1]]
        actual, _ = localDataService._generateStateId(mock.Mock())
        assert actual == "9618b936a4419a6e"

    def test__findMatchingFileList():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
        assert actual is not None
        assert len(actual) == 1

    def test_readCalibrationIndexMissing():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readCalibrationIndex("123")) == 0

    def test_readNormalizationIndexMissing():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readNormalizationIndex("123")) == 0

    def test_writeCalibrationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "CalibrationIndex.json"
        localDataService.writeCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author")
        )
        assert os.path.exists(expectedFilePath)

        fileContent = ""
        with open(expectedFilePath, "r") as indexFile:
            fileContent = indexFile.read()
        os.remove(expectedFilePath)
        assert len(fileContent) > 0

        actualEntries = parse_raw_as(List[CalibrationIndexEntry], fileContent)
        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

        # def test_writeNormalizationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
        localDataService._constructNormalizationCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "NormalizationIndex.json"
        localDataService.writeNormalizationIndexEntry(
            NormalizationIndexEntry(
                runNumber="57514", backgroundRunNumber="58813", comments="test comment", author="test author"
            )
        )
        assert os.path.exists(expectedFilePath)

        fileContent = ""
        with open(expectedFilePath, "r") as indexFile:
            fileContent = indexFile.read()
        os.remove(expectedFilePath)
        assert len(fileContent) > 0

        actualEntries = parse_raw_as(List[NormalizationIndexEntry], fileContent)
        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def test_readCalibrationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "CalibrationIndex.json"
        localDataService.writeCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author")
        )
        actualEntries = localDataService.readCalibrationIndex("57514")
        os.remove(expectedFilePath)

        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def test_readNormalizationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
        localDataService._constructNormalizationCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "NormalizationIndex.json"
        localDataService.writeNormalizationIndexEntry(
            NormalizationIndexEntry(
                runNumber="57514", backgroundRunNumber="58813", comments="test comment", author="test author"
            )
        )
        actualEntries = localDataService.readNormalizationIndex("57514")
        os.remove(expectedFilePath)

        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_readWriteCalibrationRecord_version_numbers():
        testCalibrationRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            # WARNING: 'writeCalibrationRecord' modifies <incoming record>.version,
            #   and <incoming record>.calibrationFittingIngredients.version.

            # write: version == 1
            localDataService.writeCalibrationRecord(testCalibrationRecord)
            actualRecord = localDataService.readCalibrationRecord("57514")
            assert actualRecord.version == 1
            assert actualRecord.calibrationFittingIngredients.version == 1
            # write: version == 2
            localDataService.writeCalibrationRecord(testCalibrationRecord)
            actualRecord = localDataService.readCalibrationRecord("57514")
            assert actualRecord.version == 2
            assert actualRecord.calibrationFittingIngredients.version == 2
        assert actualRecord.runNumber == "57514"
        assert actualRecord == testCalibrationRecord

    def test_readWriteCalibrationRecord_with_version():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeCalibrationRecord(
                CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            )
            actualRecord = localDataService.readCalibrationRecord("57514", "1")
        assert actualRecord.runNumber == "57514"
        assert actualRecord.version == 1

    def test_readWriteCalibrationRecord():
        testCalibrationRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeCalibrationRecord(testCalibrationRecord)
            actualRecord = localDataService.readCalibrationRecord("57514")
        assert actualRecord.runNumber == "57514"
        assert actualRecord == testCalibrationRecord

    def test_readWriteNormalizationRecord_version_numbers():
        testNormalizationRecord = NormalizationRecord.parse_raw(
            Resource.read("inputs/normalization/NormalizationRecord.json")
        )
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
            localDataService._constructNormalizationCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            # WARNING: 'writeNormalizationRecord' modifies <incoming record>.version,
            # and <incoming record>.normalization.version.

            # write: version == 1
            localDataService.writeNormalizationRecord(testNormalizationRecord)
            actualRecord = localDataService.readNormalizationRecord("57514")
            assert actualRecord.version == 1
            assert actualRecord.calibration.version == 1
            # write: version == 2
            localDataService.writeNormalizationRecord(testNormalizationRecord)
            actualRecord = localDataService.readNormalizationRecord("57514")
            assert actualRecord.version == 2
            assert actualRecord.calibration.version == 2
        assert actualRecord.runNumber == "57514"
        assert actualRecord == testNormalizationRecord

    def test_readWriteNormalizationRecord():
        testNormalizationRecord = NormalizationRecord.parse_raw(
            Resource.read("inputs/normalization/NormalizationRecord.json")
        )
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
            localDataService._constructNormalizationCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeNormalizationRecord(testNormalizationRecord)
            actualRecord = localDataService.readNormalizationRecord("57514")
        assert actualRecord.runNumber == "57514"
        assert actualRecord == testNormalizationRecord

    def test_getCalibrationRecordPath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationRecordPath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/CalibrationRecord.json"

    def test_getNormalizationRecordPath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
        localDataService._constructNormalizationCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getNormalizationRecordPath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/NormalizationRecord.json"

    def test_extractFileVersion():
        localDataService = LocalDataService()
        actualVersion = localDataService._extractFileVersion("Powder/1234/v_4/CalibrationRecord.json")
        assert actualVersion == 4

    def test__getFileOfVersion():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "/v_1/CalibrationRecord.json",
            "/v_3/CalibrationRecord.json",
        ]
        actualFile = localDataService._getFileOfVersion("/v_*/CalibrationRecord", 3)
        assert actualFile == "/v_3/CalibrationRecord.json"

    def test__getLatestFile():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "Powder/1234/v_1/CalibrationRecord.json",
            "Powder/1234/v_2/CalibrationRecord.json",
        ]
        actualFile = localDataService._getLatestFile("Powder/1234/v_*/CalibrationRecord.json")
        assert actualFile == "Powder/1234/v_2/CalibrationRecord.json"

    def test_writeCalibrationReductionResult():
        from snapred.backend.data.LocalDataService import LocalDataService as LocalDataService2

        localDataService = LocalDataService2()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")

        filename = localDataService.writeCalibrationReductionResult("123", "ws", dryrun=True)
        assert filename.endswith("tests/resources/outputs/123/ws_v1.nxs")

    def test_writeCalibrationReductionResult_notdryrun():
        from snapred.backend.data.LocalDataService import LocalDataService as LocalDataService2

        localDataService = LocalDataService2()
        localDataService.groceryService = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")

        filename = localDataService.writeCalibrationReductionResult("123", "ws", dryrun=False)
        assert filename.endswith("tests/resources/outputs/123/ws_v1.nxs")
        assert localDataService.groceryService.writeWorkspace.called_once_with(filename, "ws")

    def test__isApplicableEntry_equals():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = "123"
        assert localDataService._isApplicableEntry(entry, "123")

    def test__isApplicableEntry_greaterThan():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = ">123"
        assert localDataService._isApplicableEntry(entry, "456")

    def test__isApplicableEntry_lessThan():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = "<123"
        assert localDataService._isApplicableEntry(entry, "99")

    def test__getVersionFromCalibrationIndex():
        localDataService = LocalDataService()
        localDataService.readCalibrationIndex = mock.Mock()
        localDataService.readCalibrationIndex.return_value = [mock.Mock()]
        localDataService.readCalibrationIndex.return_value[0] = CalibrationIndexEntry(
            timestamp=123, version=1, appliesTo="123", runNumber="123", comments="", author=""
        )
        actualVersion = localDataService._getVersionFromCalibrationIndex("123")
        assert actualVersion == "1"

    def test__getVersionFromNormalizationIndex():
        localDataService = LocalDataService()
        localDataService.readNormalizationIndex = mock.Mock()
        localDataService.readNormalizationIndex.return_value = [mock.Mock()]
        localDataService.readNormalizationIndex.return_value[0] = NormalizationIndexEntry(
            timestamp=123,
            version=1,
            appliesTo="123",
            runNumber="123",
            backgroundRunNumber="456",
            comments="",
            author="",
        )
        actualVersion = localDataService._getVersionFromNormalizationIndex("123")
        assert actualVersion == "1"

    def test__getCurrentCalibrationRecord():
        localDataService = LocalDataService()
        localDataService._getVersionFromCalibrationIndex = mock.Mock()
        localDataService._getVersionFromCalibrationIndex.return_value = "1"
        localDataService.readCalibrationRecord = mock.Mock()
        mockRecord = mock.Mock()
        localDataService.readCalibrationRecord.return_value = mockRecord
        actualRecord = localDataService._getCurrentCalibrationRecord("123")
        assert actualRecord == mockRecord

    def test__getCurrentNormalizationRecord():
        localDataService = LocalDataService()
        localDataService._getVersionFromNormalizationIndex = mock.Mock()
        localDataService._getVersionFromNormalizationIndex.return_value = "1"
        localDataService.readNormalizationRecord = mock.Mock()
        mockRecord = mock.Mock()
        localDataService.readNormalizationRecord.return_value = mockRecord
        actualRecord = localDataService._getCurrentNormalizationRecord("123")
        assert actualRecord == mockRecord

    def test_getCalibrationStatePath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationStatePath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/CalibrationParameters.json"

    def test_readCalibrationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.getCalibrationStatePath = mock.Mock()
        localDataService.getCalibrationStatePath.return_value = Resource.getPath(
            "ab8704b0bc2a2342/v_1/CalibrationParameters.json"
        )
        localDataService._getLatestFile = mock.Mock()
        localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        testCalibrationState = Calibration.parse_raw(Resource.read("inputs/calibration/CalibrationParameters.json"))
        actualState = localDataService.readCalibrationState("57514")

        assert actualState == testCalibrationState

    def test_readNormalizationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("ab8704b0bc2a2342", None)
        localDataService.getNormalizationStatePath = mock.Mock()
        localDataService.getNormalizationStatePath.return_value = Resource.getPath(
            "ab8704b0bc2a2342/v_1/NormalizationParameters.json"
        )
        localDataService._getLatestFile = mock.Mock()
        localDataService._getLatestFile.return_value = Resource.getPath(
            "inputs/normalization/NormalizationParameters.json"
        )
        localDataService._getCurrentNormalizationRecord = mock.Mock()
        testNormalizationState = Normalization.parse_raw(
            Resource.read("inputs/normalization/NormalizationParameters.json")
        )
        actualState = localDataService.readNormalizationState("57514")
        assert actualState == testNormalizationState

    def test_writeCalibrationState():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            print(f"TEMP DIR = {tempdir}")
            localDataService = LocalDataService()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService._getCurrentCalibrationRecord = mock.Mock()
            localDataService._getCurrentCalibrationRecord.return_value = Calibration.construct({"name": "test"})
            calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
            localDataService.writeCalibrationState("123", calibration)
            assert os.path.exists(tempdir + "/v_1/CalibrationParameters.json")

    def test_writeNormalizationState():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._constructNormalizationCalibrationStatePath = mock.Mock()
            localDataService._constructNormalizationCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService._getCurrentNormalizationRecord = mock.Mock()
            localDataService._getCurrentNormalizationRecord.return_value = Normalization.construct({"name": "test"})
            with Resource.open("/inputs/normalization/NormalizationParameters.json", "r") as f:
                normalization = Normalization.parse_raw(f.read())
            localDataService.writeNormalizationState("123", normalization)
            assert os.path.exists(tempdir + "/v_1/NormalizationParameters.json")

    def test_initializeState():
        # Test 'initializeState'; test basic functionality.

        localDataService = LocalDataService()
        localDataService._readPVFile = mock.Mock()
        pvFileMock = mock.Mock()
        pvFileMock.get.side_effect = [[1], [2], [1.1], [1.2], [1], [1], [2], [1.1], [1.2], [1], [1], [2]]
        localDataService._readPVFile.return_value = pvFileMock
        testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))

        localDataService.readInstrumentConfig = mock.Mock()
        localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
        localDataService.writeCalibrationState = mock.Mock()
        actual = localDataService.initializeState("123", "test")
        actual.creationDate = testCalibrationData.creationDate

        assert actual == testCalibrationData

    # NOTE: This test fails on analysis because the instrument home actually does exist!
    @pytest.mark.skipif(
        IS_ON_ANALYSIS_MACHINE, reason="This test fails on analysis because the instrument home actually does exist!"
    )
    def test_badPaths():
        """This verifies that a broken configuration (from production) can't find all of the files"""
        # get a handle on the service
        service = LocalDataService()
        service.verifyPaths = True  # override test setting
        prevInstrumentHome = Config._config["instrument"]["home"]
        Config._config["instrument"]["home"] = "/this/path/does/not/exist"
        with pytest.raises(FileNotFoundError):
            service.readInstrumentConfig()
        Config._config["instrument"]["home"] = prevInstrumentHome
        service.verifyPaths = False  # put the setting back

    def test_readSamplePaths():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "/sample1.json",
            "/sample2.json",
        ]
        result = localDataService.readSamplePaths()
        assert len(result) == 2
        assert "/sample1.json" in result
        assert "/sample2.json" in result

    def test_readNoSamplePaths():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = []

        with pytest.raises(RuntimeError) as e:
            localDataService.readSamplePaths()
        assert "No samples found" in str(e.value)

    def test_readDefaultGroupingMap():
        service = LocalDataService()
        savePath = Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"]
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = Resource.getPath(
            "inputs/pixel_grouping/"
        )
        groupingMap = None
        groupingMap = service._readDefaultGroupingMap()
        assert groupingMap.isDefault
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = savePath

    def test_readGroupingMap_default_not_found():
        service = LocalDataService()
        savePath = Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"]
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = Resource.getPath("inputs/")
        with pytest.raises(  # noqa: PT012
            FileNotFoundError,
            match="default grouping-schema map",
        ):
            service._readDefaultGroupingMap()
        Config._config["instrument"]["calibration"]["powder"]["grouping"]["home"] = savePath

    def test_readGroupingMap_initialized_state():
        # Test that '_readGroupingMap' for an initialized state returns the state's <grouping map>.
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            service = LocalDataService()
            stateId = "ab8704b0bc2a2342"
            stateRoot = Path(f"{tempdir}/{stateId}")
            service._constructCalibrationStatePath = mock.Mock()
            service._constructCalibrationStatePath.return_value = str(stateRoot)
            stateRoot.mkdir()
            shutil.copy(Path(Resource.getPath("inputs/pixel_grouping/groupingMap.json")), stateRoot)
            groupingMap = service._readGroupingMap(stateId)
            assert groupingMap.stateId == stateId

    def test_readGroupingFiles():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.side_effect = lambda path, throws: [  # noqa: ARG005
            f"/group1.{path.split('/')[-1].split('.')[-1]}",
            f"/group2.{path.split('/')[-1].split('.')[-1]}",
        ]
        result = localDataService.readGroupingFiles()
        # 6 because there are 3 file types and the mock returns 2 files per
        assert len(result) == 6
        assert result == [f"/group{x}.{ext}" for ext in ["xml", "nxs", "hdf"] for x in [1, 2]]

    def test_readNoGroupingFiles():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = []
        try:
            localDataService.readGroupingFiles()
            pytest.fail("Should have thrown an exception")
        except RuntimeError:
            assert True

    def test_readFocusGroups():
        # test of `LocalDataService.readFocusGroups`
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.side_effect = lambda path, throws: [  # noqa: ARG005
            f"/group1.{path.split('/')[-1].split('.')[-1]}",
            f"/group2.{path.split('/')[-1].split('.')[-1]}",
        ]
        result = localDataService.readFocusGroups()
        # 6 because there are 3 file types and the mock returns 2 files per
        assert len(result) == 6
        assert list(result.keys()) == [f"/group{x}.{ext}" for ext in ["xml", "nxs", "hdf"] for x in [1, 2]]
        assert list(result.values()) == [
            FocusGroup(name=x.split("/")[-1].split(".")[0], definition=x) for x in list(result.keys())
        ]

    @mock.patch("os.path.exists", return_value=True)
    def test_writeCalibrantSample_failure(mock1):  # noqa: ARG001
        localDataService = LocalDataService()
        sample = mock.MagicMock()
        sample.name = "apple"
        sample.unique_id = "banana"
        with pytest.raises(ValueError) as e:  # noqa: PT011
            localDataService.writeCalibrantSample(sample)
        assert sample.name in str(e.value)
        assert sample.unique_id in str(e.value)
        assert Config["samples.home"] in str(e.value)

    def test_writeCalibrantSample_success():  # noqa: ARG002
        localDataService = LocalDataService()
        sample = mock.MagicMock()
        sample.name = "apple"
        sample.unique_id = "banana"
        sample.json.return_value = "I like to eat, eat, eat"
        temp = Config._config["samples"]["home"]
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            Config._config["samples"]["home"] = tempdir
            # mock_os_join.return_value = f"{tempdir}{sample.name}_{sample.unique_id}"
            filePath = f"{tempdir}/{sample.name}_{sample.unique_id}.json"
            localDataService.writeCalibrantSample(sample)
            assert os.path.exists(filePath)
        Config._config["samples"]["home"] = temp

    @mock.patch("os.path.exists", return_value=True)
    def test_readCalibrantSample(mock1):  # noqa: ARG001
        localDataService = LocalDataService()

        result = localDataService.readCalibrantSample(
            Resource.getPath("inputs/calibrantSamples/Silicon_NIST_640D_001.json")
        )
        assert type(result) == CalibrantSamples
        assert result.name == "Silicon_NIST_640D"

    @mock.patch("os.path.exists", return_value=True)
    def test_readCifFilePath(mock1):  # noqa: ARG001
        localDataService = LocalDataService()

        result = localDataService.readCifFilePath("testid")
        assert result == "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/EntryWithCollCode52054_diamond.cif"

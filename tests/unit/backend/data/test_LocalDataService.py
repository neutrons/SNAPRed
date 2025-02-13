import datetime
import functools
import importlib
import json
import logging
import os
import re
import socket
import tempfile
import time
import typing
import unittest.mock as mock
from collections.abc import Mapping
from contextlib import ExitStack
from pathlib import Path
from random import randint, shuffle
from typing import List, Literal, Set

import h5py
import numpy as np
import pydantic
import pytest
from mantid.api import ITableWorkspace, MatrixWorkspace, Run
from mantid.dataobjects import MaskWorkspace
from mantid.kernel import amend_config
from mantid.simpleapi import (
    CloneWorkspace,
    CompareWorkspaces,
    CreateGroupingWorkspace,
    CreateSampleWorkspace,
    DeleteWorkspaces,
    EditInstrumentGeometry,
    GroupWorkspaces,
    LoadEmptyInstrument,
    LoadInstrument,
    RenameWorkspaces,
    SaveDiffCal,
    mtd,
)
from util.Config_helpers import Config_override
from util.dao import DAOFactory
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors
from util.state_helpers import reduction_root_redirect, state_root_redirect

from snapred.backend.dao import StateConfig
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.LiveMetadata import LiveMetadata
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    CreateNormalizationRecordRequest,
)
from snapred.backend.dao.state import DetectorState
from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.Indexer import IndexerType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.meta.Config import Config, Resource
from snapred.meta.InternalConstants import ReservedRunNumber, ReservedStateId
from snapred.meta.mantid.WorkspaceNameGenerator import (
    ValueFormatter as wnvf,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceName,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceNameGenerator as wng,
)
from snapred.meta.mantid.WorkspaceNameGenerator import (
    WorkspaceType as wngt,
)
from snapred.meta.redantic import parse_file_as, write_model_pretty

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)
ThisService = "snapred.backend.data.LocalDataService."

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

# NOTE: Devs, never changing the comparison value:
UNCHANGING_STATE_ID = "9618b936a4419a6e"
ENDURING_STATE_ID = "ab8704b0bc2a2342"


# NOTE: The state id values above are the result of how we define two specific DetectorState objects:
def mockDetectorState(runId: str) -> DetectorState:
    if runId == "12345":
        return DetectorState(arc=(0.1, 0.1), wav=0.1, freq=0.1, guideStat=1, lin=(0.1, 0.1))
    elif runId == "123":
        return DetectorState(arc=(1.0, 2.0), wav=1.1, freq=1.2, guideStat=1, lin=(1.0, 2.0))
    elif runId == "67890":
        return DetectorState(arc=(0.2, 0.2), wav=0.2, freq=0.2, guideStat=1, lin=(0.2, 0.2))
    return None


def mockPVFile(detectorState: DetectorState) -> mock.Mock:
    # See also: `tests/unit/backend/data/util/test_PV_logs_util.py`.

    # Note: `PV_logs_util.mappingFromNeXusLogs` will open the 'entry/DASlogs' group,
    #   so this `dict` mocks the HDF5 group, not the PV-file itself.

    # For the HDF5-file, each key requires the "/value" suffix.
    dict_ = {
        "run_number/value": "123456",
        "start_time/value": "2023-06-14T14:06:40.429048667",
        "end_time/value": "2023-06-14T14:07:56.123123123",
        "BL3:Chop:Skf1:WavelengthUserReq/value": [detectorState.wav],
        "det_arc1/value": [detectorState.arc[0]],
        "det_arc2/value": [detectorState.arc[1]],
        "BL3:Det:TH:BL:Frequency/value": [detectorState.freq],
        "BL3:Mot:OpticsPos:Pos/value": [detectorState.guideStat],
        "det_lin1/value": [detectorState.lin[0]],
        "det_lin2/value": [detectorState.lin[1]],
    }

    def del_item(key: str):
        # bypass <class>.__delitem__
        del dict_[key]

    mock_ = mock.MagicMock(spec=h5py.Group)

    mock_.get = lambda key, default=None: dict_.get(key, default)
    mock_.del_item = del_item

    # Use of the h5py.File starts with access to the "entry/DASlogs" group:
    mock_.__getitem__.side_effect = lambda key: mock_ if key == "entry/DASlogs" else dict_[key]

    mock_.__contains__.side_effect = dict_.__contains__
    mock_.keys.side_effect = dict_.keys
    return mock_


def mockNeXusLogsMapping(detectorState: DetectorState) -> mock.Mock:
    # See also: `tests/unit/backend/data/util/test_PV_logs_util.py`.

    dict_ = {
        "run_number": "123456",
        "start_time": datetime.datetime.fromisoformat("2023-06-14T14:06:40.429048"),
        "end_time": datetime.datetime.fromisoformat("2023-06-14T14:07:56.123123"),
        "proton_charge": 1000.0,
        "BL3:Chop:Skf1:WavelengthUserReq": [detectorState.wav],
        "det_arc1": [detectorState.arc[0]],
        "det_arc2": [detectorState.arc[1]],
        "BL3:Det:TH:BL:Frequency": [detectorState.freq],
        "BL3:Mot:OpticsPos:Pos": [detectorState.guideStat],
        "det_lin1": [detectorState.lin[0]],
        "det_lin2": [detectorState.lin[1]],
    }
    return mockNeXusLogsMappingFromDict(dict_)


def mockNeXusLogsMappingFromDict(map_: dict) -> mock.Mock:
    dict_ = map_

    def del_item(key: str):
        # bypass <class>.__delitem__
        del dict_[key]

    mock_ = mock.MagicMock(spec=Mapping)

    mock_.get = lambda key, default=None: dict_.get(key, default)
    mock_.del_item = del_item
    mock_.__getitem__.side_effect = dict_.__getitem__
    mock_.__contains__.side_effect = dict_.__contains__
    mock_.keys.side_effect = dict_.keys
    return mock_


@pytest.fixture(autouse=True)
def _capture_logging(monkeypatch):
    # For some reason pytest 'caplog' doesn't work with the SNAPRed logging setup.  (TODO: fix this!)
    # This patch bypasses the issue, by renaming and
    # patching the `LocalDataService` module's logger to a standard python `Logger`.
    defaultLogger = logging.getLogger(LocalDataServiceModule.__name__ + "_patch")
    defaultLogger.propagate = True
    monkeypatch.setattr(LocalDataServiceModule, "logger", defaultLogger)


fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")


def entryFromRecord(record):
    return IndexEntry(
        runNumber=record.runNumber,
        useLiteMode=record.useLiteMode,
        appliesTo=record.runNumber,
        comments="test comment",
        author="test author",
        version=record.version,
    )


### GENERALIZED METHODS FOR TESTING NORMALIZATION / CALIBRATION METHODS ###
# Note: the REDUCTION workflow does not use the Indexer system except indirectly.


def do_test_index_missing(workflow):
    # NOTE this is already covered by Indexer tests,
    # but it existed and didn't hurt to retain
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        assert len(getattr(localDataService, f"read{workflow}Index")("123", True)) == 0


def do_test_workflow_indexer(workflow):
    # verify the correct indexer is being returned
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock()
    for useLiteMode in [True, False]:
        getattr(localDataService, f"{workflow.lower()}Indexer")("xyz", useLiteMode)
        localDataService.indexer.assert_called_once_with("xyz", useLiteMode, IndexerType(workflow))
        localDataService.indexer.reset_mock()


def do_test_read_index(workflow):
    # verify that calls to read index call out to the indexer
    mockIndex = ["nope"]
    mockIndexer = mock.Mock(getIndex=mock.Mock(return_value=mockIndex))
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        ans = getattr(localDataService, f"read{workflow}Index")("xyz", useLiteMode)
        assert ans == mockIndex


def do_test_read_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        version = randint(1, 20)
        res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode, version)
        assert res == mockIndexer.readRecord.return_value
        mockIndexer.readRecord.assert_called_with(version)


def do_test_read_record_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    latestVersion = randint(20, 120)
    mockIndexer = mock.Mock(latestApplicableVersion=mock.Mock(return_value=latestVersion))
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode)  # NOTE no version
        mockIndexer.latestApplicableVersion.assert_called_with("xyz")
        mockIndexer.readRecord.assert_called_with(latestVersion)
        assert res == mockIndexer.readRecord.return_value


def do_test_write_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the methods inside the indexer service
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.indexer = mock.Mock(return_value=mockIndexer)
    for useLiteMode in [True, False]:
        record = globals()[f"{workflow}Record"].model_construct(
            runNumber="xyz",
            useLiteMode=useLiteMode,
            workspaces={},
            version=randint(1, 120),
            calculationParameters=mock.Mock(),
        )
        getattr(localDataService, f"write{workflow}Record")(record)
        mockIndexer.writeRecord.assert_called_with(record)
        mockIndexer.writeParameters.assert_called_with(record.calculationParameters)


def do_test_read_state_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    paramFactory = getattr(DAOFactory, f"{workflow.lower()}Parameters")
    localDataService = LocalDataService()
    localDataService.calibrationExists = mock.Mock(return_value=True)
    versions = list(range(randint(10, 20)))
    shuffle(versions)
    for version in versions:
        for useLiteMode in [True, False]:
            with state_root_redirect(localDataService) as tmpRoot:
                expectedState = paramFactory(version=version)
                indexer = localDataService.indexer("xyz", useLiteMode, workflow)
                tmpRoot.saveObjectAt(expectedState, indexer.parametersPath(version))
                assert indexer.parametersPath(version).exists()
                actualState = getattr(localDataService, f"read{workflow}State")("xyz", useLiteMode, version)
            assert actualState == expectedState


def do_test_read_state_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    currentVersion = randint(20, 120)
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with state_root_redirect(localDataService) as tmpRoot:
            expectedState = getattr(DAOFactory, f"{workflow.lower()}Parameters")()
            indexer = localDataService.indexer("xyz", useLiteMode, workflow)
            tmpRoot.saveObjectAt(expectedState, indexer.parametersPath(currentVersion))
            indexer.index = {
                currentVersion: mock.MagicMock(appliesTo="123", version=currentVersion)
            }  # NOTE manually update indexer
            indexer.dirVersions = [currentVersion]  # NOTE manually update indexer
            actualState = getattr(localDataService, f"read{workflow}State")(
                "123", useLiteMode, VersionState.LATEST
            )  # NOTE no version
        assert actualState == expectedState


### TESTS OF MISCELLANEOUS METHODS ###


def test_fileExists_yes():
    # create a temp file that exists, and verify it exists
    with tempfile.NamedTemporaryFile(suffix=".biscuit") as existent:
        assert LocalDataService().fileExists(existent.name)


def test_fileExists_no():
    # assert that a file that does not exist, does not exist
    with tempfile.TemporaryDirectory() as tmpdir:
        nonexistent = tmpdir + "/0x0f.biscuit"
        assert not os.path.isfile(nonexistent)
        assert not LocalDataService().fileExists(nonexistent)


def _readInstrumentParameters():
    instrumentParameters = None
    with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
        instrumentParameters = json.loads(file.read())
    return instrumentParameters


def test__readInstrumentConfig():
    localDataService = LocalDataService()
    localDataService._readInstrumentParameters = _readInstrumentParameters
    actual = localDataService._readInstrumentConfig()
    assert actual is not None
    assert actual.version == "1.4"
    assert actual.name == "SNAP"


def test_readInstrumentParameters():
    localDataService = LocalDataService()
    localDataService._instrumentConfigPath = Resource.getPath("inputs/SNAPInstPrm.json")
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


def test_readStateConfig_default():
    # readstateConfig will load the default parameters file
    groupingMap = DAOFactory.groupingMap_SNAP()
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        parameters = DAOFactory.calibrationParameters("57514", True, indexer.defaultVersion())
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(indexer.defaultVersion()))

        indexer.index = {
            VersionState.DEFAULT: mock.MagicMock(appliesTo="57514", version=indexer.defaultVersion())
        }  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == DAOFactory.magical_state_id


def test_readStateConfig_previous():
    # readStateConfig will load the previous version's parameters file
    version = randint(2, 10)
    groupingMap = DAOFactory.groupingMap_SNAP()
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == DAOFactory.magical_state_id


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    groupingMap = DAOFactory.groupingMap_SNAP()
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        actual = localDataService.readStateConfig("57514", True)
    expectedMap = DAOFactory.groupingMap_SNAP()
    assert actual.groupingMap == expectedMap


def test_readStateConfig_invalid_grouping_map():
    # Test that the attached grouping-schema map's 'stateId' is checked.
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    groupingMap = DAOFactory.groupingMap_SNAP(DAOFactory.nonsense_state_id)
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
        with pytest.raises(  # noqa: PT012
            RuntimeError,
            match="the state configuration's grouping map must have the same 'stateId' as the configuration",
        ):
            localDataService.readStateConfig("57514", True)


def test_readStateConfig_calls_prepareStateRoot():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    groupingMap = DAOFactory.groupingMap_SNAP()
    expected = DAOFactory.calibrationParameters("57514", True, version)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=expected.instrumentState.id.hex) as tmpRoot:
        indexer = localDataService.calibrationIndexer("57514", True)
        tmpRoot.saveObjectAt(expected, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        assert not localDataService._groupingMapPath(tmpRoot.stateId).exists()
        localDataService._prepareStateRoot = mock.Mock(
            side_effect=lambda x: tmpRoot.saveObjectAt(  # noqa ARG005
                groupingMap,
                localDataService._groupingMapPath(tmpRoot.stateId),
            )
        )
        actual = localDataService.readStateConfig("57514", True)
        assert localDataService._groupingMapPath(tmpRoot.stateId).exists()
    assert actual is not None
    assert actual.stateId.hex == tmpRoot.stateId
    localDataService._prepareStateRoot.assert_called_once()


def test_getUniqueTimestamp():
    localDataService = LocalDataService()
    numberToGenerate = 10
    tss = set([localDataService.getUniqueTimestamp() for n in range(numberToGenerate)])

    # generated values should not be 'None' or 'int', they must be 'float'
    for ts in tss:
        assert isinstance(ts, float)
    # generated values shall be distinct
    assert len(tss) == numberToGenerate

    # generated values should have distinct `struct_time`:
    #   this check ensures that generated values differ by at least 1 second
    ts_structs = set([time.gmtime(ts) for ts in tss])
    assert len(ts_structs) == numberToGenerate


@mock.patch("socket.gethostbyaddr")
def test_hasLiveDataConnection(mockGetHostByAddr):
    liveDataHostname = "bl3-daq1.sns.gov"
    liveDataIPV4 = "10.111.6.150"
    mockGetHostByAddr.return_value = (liveDataHostname, [], [liveDataIPV4])
    instance = LocalDataService()
    assert instance.hasLiveDataConnection()
    mockGetHostByAddr.assert_called_once_with(liveDataHostname)


@mock.patch("socket.gethostbyaddr")
def test_hasLiveDataConnection_no_connection(mockGetHostByAddr):
    mockGetHostByAddr.side_effect = RuntimeError("no live connection")
    instance = LocalDataService()
    assert not instance.hasLiveDataConnection()


def test_prepareStateRoot_creates_state_root_directory():
    # Test that the <state root> directory is created when it doesn't exist.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = DAOFactory.groupingMap_SNAP()
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert not localDataService.constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService.constructCalibrationStateRoot().exists()


def test_prepareStateRoot_existing_state_root():
    # Test that an already existing <state root> directory is not an error.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        localDataService.constructCalibrationStateRoot().mkdir()
        defaultGroupingMap = DAOFactory.groupingMap_SNAP()
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        assert localDataService.constructCalibrationStateRoot().exists()
        localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_writes_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the `StateConfig.groupingMap` is written to the directory.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = DAOFactory.groupingMap_SNAP()
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        assert not localDataService._groupingMapPath(stateId).exists()
        localDataService._prepareStateRoot(stateId)
        assert localDataService._groupingMapPath(stateId).exists()


def test_prepareStateRoot_sets_grouping_map_stateid():
    # Test that the first time a <state root> directory is initialized,
    #   the 'stateId' of the `StateConfig.groupingMap` is set to match that of the state.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMap = DAOFactory.groupingMap_SNAP()
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)

        localDataService._prepareStateRoot(stateId)

        groupingMap = parse_file_as(GroupingMap, localDataService._groupingMapPath(stateId))
    assert groupingMap.stateId == stateId


def test_prepareStateRoot_no_default_grouping_map():
    # Test that the first time a <state root> directory is initialized,
    #   the 'defaultGroupingMap.json' at Config['instrument.calibration.powder.grouping.home']
    #   is required to exist.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId):
        defaultGroupingMapFilePath = Resource.getPath("inputs/pixel_grouping/does_not_exist.json")
        with pytest.raises(  # noqa: PT012
            FileNotFoundError,
            match=f'required default grouping-schema map "{defaultGroupingMapFilePath}" does not exist',
        ):
            localDataService._defaultGroupingMapPath = mock.Mock(return_value=Path(defaultGroupingMapFilePath))
            localDataService._prepareStateRoot(stateId)


def test_prepareStateRoot_does_not_overwrite_grouping_map():
    # If a 'groupingMap.json' file already exists at the <state root> directory,
    #   it should not be overwritten.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        localDataService.constructCalibrationStateRoot().mkdir()

        # Write a 'groupingMap.json' file to the <state root>, but with a different stateId;
        #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
        otherStateId = "bbbbaaaabbbbeeee"
        groupingMap = DAOFactory.groupingMap_SNAP(otherStateId)
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(stateId))

        defaultGroupingMap = DAOFactory.groupingMap_SNAP(stateId)
        localDataService._readDefaultGroupingMap = mock.Mock(return_value=defaultGroupingMap)
        localDataService._prepareStateRoot(stateId)

        groupingMap = parse_file_as(GroupingMap, localDataService._groupingMapPath(stateId))
    assert groupingMap.stateId == otherStateId


def test_writeGroupingMap_relative_paths():
    # Test that '_writeGroupingMap' preserves relative-path information.
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        with Config_override(
            "instrument.calibration.powder.grouping.home",
            tmpRoot.path().parent,
        ):
            defaultGroupingMap = DAOFactory.groupingMap_SNAP(stateId)
            defaultGroupingMapFilePath = localDataService._defaultGroupingMapPath()
            write_model_pretty(defaultGroupingMap, defaultGroupingMapFilePath)

        # Write a 'groupingMap.json' file to the <state root>, with the _correct_ stateId;
        #   note that the _value_ of the stateId field is _not_ validated at this stage, except for its format.
        groupingMap = DAOFactory.groupingMap_SNAP(stateId)
        groupingMapFilePath = localDataService._groupingMapPath(stateId)
        tmpRoot.saveObjectAt(groupingMap, groupingMapFilePath)

        # read it back
        groupingMap = parse_file_as(GroupingMap, groupingMapFilePath)
        defaultGroupingMap = parse_file_as(GroupingMap, defaultGroupingMapFilePath)

    # test that relative paths are preserved
    relativePathCount = 0
    for n, focusGroup in enumerate(groupingMap.liteFocusGroups):
        assert focusGroup == defaultGroupingMap.liteFocusGroups[n]
        if not Path(focusGroup.definition).is_absolute():
            relativePathCount += 1
    for n, focusGroup in enumerate(groupingMap.nativeFocusGroups):
        assert focusGroup == defaultGroupingMap.nativeFocusGroups[n]
        if not Path(focusGroup.definition).is_absolute():
            relativePathCount += 1
    assert relativePathCount > 0


@mock.patch("pathlib.Path.exists")
@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists(GetIPTS, pathExists):  # noqa ARG002
    pathExists.return_value = True
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        tmpRoot.path().mkdir()
        runNumber = "654321"
        assert localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_stupid_number(mockGetIPTS):
    localDataService = LocalDataService()

    # try with a non-number
    runNumber = "fruitcake"
    assert not localDataService.checkCalibrationFileExists(runNumber)
    mockGetIPTS.assert_not_called()
    mockGetIPTS.reset_mock()

    # try with a too-small number
    runNumber = "7"
    assert not localDataService.checkCalibrationFileExists(runNumber)
    mockGetIPTS.assert_not_called()


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_not(GetIPTS):  # noqa ARG002
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        nonExistentPath = tmpRoot.path() / "1755"
        assert not nonExistentPath.exists()
        runNumber = "654321"
        assert not localDataService.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_no_PV_file(GetIPTS):  # noqa ARG002
    instance = LocalDataService()
    stateId = ENDURING_STATE_ID  # noqa: F841
    runNumber = "654321"
    with mock.patch.object(instance, "generateStateId") as mockGenerateStateId:
        # WARNING: the actual exception would normally be re-routed to `StateValidationException`.
        mockGenerateStateId.side_effect = FileNotFoundError("No PV-file")
        assert not instance.checkCalibrationFileExists(runNumber)


@mock.patch(ThisService + "GetIPTS")
def test_calibrationFileExists_no_PV_file_exception_routing(GetIPTS):  # noqa ARG002
    instance = LocalDataService()
    runNumber = "654321"
    with pytest.raises(StateValidationException):
        instance.checkCalibrationFileExists(runNumber)


@mock.patch("pathlib.Path.exists")
@mock.patch(ThisService + "GetIPTS")
def test_getIPTS(mockGetIPTS, mockPathExists):
    mockPathExists.return_value = True
    mockGetIPTS.return_value = "nowhere/"
    localDataService = LocalDataService()
    runNumber = "123456"
    res = localDataService.getIPTS(runNumber)
    assert res == mockGetIPTS.return_value
    mockGetIPTS.assert_called_with(
        RunNumber=runNumber,
        Instrument=Config["instrument.name"],
    )
    res = localDataService.getIPTS(runNumber, "CRACKLE")
    assert res == mockGetIPTS.return_value
    mockGetIPTS.assert_called_with(
        RunNumber=runNumber,
        Instrument="CRACKLE",
    )


# NOTE this test calls `GetIPTS` (via `getIPTS`) with no mocks
# this is intentional, to ensure it is being called correctly
@mock.patch("pathlib.Path.exists")
def test_getIPTS_cache(mockPathExists):
    mockPathExists.return_value = True
    localDataService = LocalDataService()
    localDataService.getIPTS.cache_clear()
    assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=0, maxsize=128, currsize=0)

    # test data
    instrument = "SNAP"
    runNumber = "123"
    key = (runNumber, instrument)
    correctIPTS = Path(Resource.getPath("inputs/testInstrument/IPTS-456"))
    incorrectIPTS = Path(Resource.getPath("inputs/testInstrument/IPTS-789"))

    # direct GetIPTS to look in the exact folder where it should look
    # it is very stupid, so if you don't tell it exactly then it won't look there
    with amend_config(data_dir=str(correctIPTS / "nexus")):
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)

        # call again and make sure the cached value is being returned
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep

        # WARNING: For this test: mocking `GetIPTS` at the module level is also possible,
        #   but this was not implemented correctly previously.
        #   If that approach is taken, the important thing is to make sure
        #   that the mock is _removed_ at the end of the test.
        #   Note that `localDataService.GetIPTS` is not the same thing as
        #   `LocalDataService <module>.GetIPTS`.
        #   For these tests, the @Singleton aspect of the class is removed,
        #   but that doesn't mean that changes to the module won't affect
        #   other tests.

        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=1, misses=1, maxsize=128, currsize=1)

    # now try it again, but with another IPTS directory
    with amend_config(data_dir=str(incorrectIPTS / "nexus")):
        # previous correct value should still be the cached value
        res = localDataService.getIPTS(*key)
        assert res == str(correctIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=2, misses=1, maxsize=128, currsize=1)

        # clear the cache,  make sure the new value is being returned
        localDataService.getIPTS.cache_clear()
        res = localDataService.getIPTS(*key)
        assert res == str(incorrectIPTS) + os.sep
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)


@mock.patch(ThisService + "GetIPTS")
def test_createNeutronFilePath(mockGetIPTS):
    instance = LocalDataService()
    mockGetIPTS.return_value = "IPTS-TEST/"
    runNumbers = {"lite": "12345", "native": "67890"}

    for mode in ["lite", "native"]:
        runNumber = runNumbers[mode]  # REMINDER: `LocalDataService.getIPTS` uses `@lru_cache`.
        expected = Path(
            mockGetIPTS.return_value + Config[f"nexus.{mode}.prefix"] + runNumber + Config[f"nexus.{mode}.extension"]
        )
        actual = instance.createNeutronFilePath(runNumber, mode == "lite")
        mockGetIPTS.assert_called_once_with(RunNumber=runNumber, Instrument=Config["instrument.name"])
        assert actual == expected
        mockGetIPTS.reset_mock()


def test_workspaceIsInstance(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    # Create a sample workspace.
    testWS0 = "test_ws"
    LoadEmptyInstrument(
        Filename=fakeInstrumentFilePath,
        OutputWorkspace=testWS0,
    )
    # Assign the required sample log values
    detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    addInstrumentLogs(testWS0, **getInstrumentLogDescriptors(detectorState1))

    assert mtd.doesExist(testWS0)
    cleanup_workspace_at_exit(testWS0)
    assert localDataService.workspaceIsInstance(testWS0, MatrixWorkspace)

    # Create diffraction-calibration table and mask workspaces.
    tableWS = "test_table"
    maskWS = "test_mask"
    createCompatibleDiffCalTable(tableWS, testWS0)
    createCompatibleMask(maskWS, testWS0)
    assert mtd.doesExist(tableWS)
    cleanup_workspace_at_exit(tableWS)
    assert mtd.doesExist(maskWS)
    cleanup_workspace_at_exit(maskWS)
    assert localDataService.workspaceIsInstance(tableWS, ITableWorkspace)
    assert localDataService.workspaceIsInstance(maskWS, MaskWorkspace)


def test_workspaceIsInstance_no_ws():
    localDataService = LocalDataService()
    # A sample workspace which doesn't exist.
    testWS0 = "test_ws"
    assert not mtd.doesExist(testWS0)
    assert not localDataService.workspaceIsInstance(testWS0, MatrixWorkspace)


def test_write_model_pretty_StateConfig_excludes_grouping_map():
    # At present there is no `writeStateConfig` method, and there is no `readStateConfig` that doesn't
    #   actually build up the `StateConfig` from its components.
    # This test verifies that `GroupingMap` is excluded from any future `StateConfig` JSON serialization.
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        # move the calculation parameters into correct folder
        indexer = localDataService.calibrationIndexer("57514", True)
        indexer.writeParameters(DAOFactory.calibrationParameters("57514", True, indexer.defaultVersion()))
        indexer.index = {indexer.defaultVersion(): mock.MagicMock(appliesTo="57514", version=indexer.defaultVersion())}

        # move the grouping map into correct folder
        write_model_pretty(DAOFactory.groupingMap_SNAP(), localDataService._groupingMapPath(tmpRoot.stateId))

        # construct the state config object
        actual = localDataService.readStateConfig("57514", True)
        # now save it to a path in the directory
        stateConfigPath = tmpRoot.path() / "stateConfig.json"
        write_model_pretty(actual, stateConfigPath)
        # read it back in and make sure there is no grouping map
        stateConfig = parse_file_as(StateConfig, stateConfigPath)
        assert stateConfig.groupingMap is None


def test_readDefaultGroupingMap():
    # test of public `readDefaultGroupingMap` method
    localDataService = LocalDataService()
    localDataService._readDefaultGroupingMap = mock.Mock()
    localDataService._readDefaultGroupingMap.return_value = "defaultGroupingMap"
    actual = localDataService.readDefaultGroupingMap()
    assert actual == "defaultGroupingMap"


def test_readRunConfig():
    # test of public `readRunConfig` method
    localDataService = LocalDataService()
    localDataService._readRunConfig = mock.Mock()
    localDataService._readRunConfig.return_value = "57514"
    actual = localDataService.readRunConfig(mock.Mock())
    assert actual is not None
    assert actual == "57514"


def test__readRunConfig():
    # Test of private `_readRunConfig` method
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value="IPTS-123")
    localDataService._instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._readRunConfig("57514")
    assert actual is not None
    assert actual.runNumber == "57514"


def test_constructPVFilePath():
    # ensure the file path points to inside the IPTS folder
    localDataService = LocalDataService()
    # mock the IPTS to point to somewhere then construct the path
    mockIPTS = Resource.getPath("inputs/testInstrument/IPTS-456")
    mockRunConfig = mock.Mock(IPTS=mockIPTS)
    localDataService._readRunConfig = mock.Mock(return_value=mockRunConfig)
    path = localDataService._constructPVFilePath("123")
    # the path should be /path/to/testInstrument/IPTS-456/nexus/SNAP_123.nxs.h5
    assert mockIPTS == str(path.parents[1])


@mock.patch("h5py.File", return_value="not None")
def test_readPVFile(h5pyMock):  # noqa: ARG001
    localDataService = LocalDataService()
    localDataService._instrumentConfig = getMockInstrumentConfig()
    localDataService._constructPVFilePath = mock.Mock()
    localDataService._constructPVFilePath.return_value = Path(Resource.getPath("./"))
    actual = localDataService._readPVFile(mock.Mock())
    assert actual is not None


def test_generateStateId():
    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    testDetectorState = mockDetectorState("12345")
    localDataService._readPVFile.return_value = mockPVFile(testDetectorState)
    actual, _ = localDataService.generateStateId("12345")
    assert actual == UNCHANGING_STATE_ID


def test_generateStateId_cache():
    localDataService = LocalDataService()
    localDataService.generateStateId.cache_clear()
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=0, maxsize=128, currsize=0
    )

    localDataService._readPVFile = mock.Mock()

    localDataService.readDetectorState = mock.Mock(side_effect=mockDetectorState)

    stateSHA1 = UNCHANGING_STATE_ID
    stateSHA2 = "fa0bb25b44874edb"

    actual, _ = localDataService.generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=0, misses=1, maxsize=128, currsize=1
    )

    # check cached value
    actual, _ = localDataService.generateStateId("12345")
    assert actual == stateSHA1
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=1, maxsize=128, currsize=1
    )

    # check a different value
    actual, _ = localDataService.generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=1, misses=2, maxsize=128, currsize=2
    )
    # ... and its cached value
    actual, _ = localDataService.generateStateId("67890")
    assert actual == stateSHA2
    assert localDataService.generateStateId.cache_info() == functools._CacheInfo(
        hits=2, misses=2, maxsize=128, currsize=2
    )


def test_generateStateId_reserved_runNumbers():
    instance = LocalDataService()
    for runNumber in ReservedRunNumber.values():
        expected = ReservedStateId.forRun(runNumber), None
        actual = instance.generateStateId(runNumber)
        assert actual == expected


def test__findMatchingFileList():
    localDataService = LocalDataService()
    localDataService._instrumentConfig = getMockInstrumentConfig()
    actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
    assert actual is not None
    assert len(actual) == 1


### TESTS OF PATH METHODS ###
def test_CheckFileAndPermission_fileIsNone():
    filePath = None
    localDS = LocalDataService()
    result = localDS.checkFileandPermission(filePath)
    assert result == (False, False)


@mock.patch("pathlib.Path.exists", return_value=False)
def test_CheckFileAndPermission_fileDoesNotExist(mockExists):  # noqa: ARG001
    filePath = Path("/some/path/to/nonexistent/file")
    localDS = LocalDataService()
    result = localDS.checkFileandPermission(filePath)
    assert result == (False, False)


@mock.patch(ThisService + "tempfile.TemporaryFile")
@mock.patch("pathlib.Path.exists", return_value=True)
def test_checkFileAndPermission_fileExistsAndWritePermission(mockExists, mockTempFile):  # noqa: ARG001
    filePath = Path("/some/path/to/file")
    localDS = LocalDataService()
    localDS._hasWritePermissionstoPath = mock.Mock()
    localDS._hasWritePermissionstoPath.return_value = True
    result = localDS.checkFileandPermission(filePath)
    assert result == (True, True)


@mock.patch(ThisService + "tempfile.TemporaryFile")
@mock.patch("pathlib.Path.exists", return_value=True)
def test__hasWritePermissionsToPath_fileExistsWithPermission(mockExists, mockTempFile):  # noqa: ARG001
    filePath = Path("/some/path/to/file")
    localDS = LocalDataService()
    result = localDS._hasWritePermissionstoPath(filePath)
    assert result is True


@mock.patch("pathlib.Path.exists", return_value=False)
def test__hasWritePermissionsToPath_fileDoesNotExist(mockExists):  # noqa: ARG001
    filePath = Path("/some/path/to/nonexistent/file")
    localDS = LocalDataService()
    result = localDS._hasWritePermissionstoPath(filePath)
    assert result is False


def test_checkWritePermissions_path_exists():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert status


@mock.patch(ThisService + "tempfile.TemporaryFile")
def test_checkWritePermissions_path_exists_no_permissions(mockTempFile):
    mockTempFile.side_effect = PermissionError
    mockTempFile.reset_mock()
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert not status
        mockTempFile.assert_called_once()


def test_checkWritePermissions_parent_exists():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        path = path / "four"
        assert not path.exists()
        status = LocalDataService().checkWritePermissions(path)
        assert status


@mock.patch(ThisService + "tempfile.TemporaryFile")
def test_checkWritePermissions_parent_exists_no_permissions(mockTempFile):
    mockTempFile.side_effect = PermissionError
    mockTempFile.reset_mock()
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        path = Path(tmpDir) / "one" / "two" / "three"
        path.mkdir(parents=True)
        assert path.exists()
        path = path / "four"
        assert not path.exists()
        status = LocalDataService().checkWritePermissions(path)
        mockTempFile.assert_called_once()
        assert not status


def test_checkWritePermissions_path_does_not_exist():
    path = Path("/does_not_exist") / "one" / "two" / "three"
    assert not path.exists()
    status = LocalDataService().checkWritePermissions(path)
    assert not status


def test_constructCalibrationStateRoot():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    ans = localDataService.constructCalibrationStateRoot(fakeState)
    assert isinstance(ans, Path)
    assert ans.parts[-1] == fakeState


def test_constructCalibrationStatePath():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._constructCalibrationStatePath(fakeState, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == "diffraction"
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService.constructCalibrationStateRoot(fakeState).parts


def test_constructNormalizationStatePath():
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._constructNormalizationStatePath(fakeState, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == "normalization"
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService.constructCalibrationStateRoot(fakeState).parts


def test_hasWritePermissionsCalibrationStateRoot():
    localDataService = LocalDataService()
    assert localDataService._hasWritePermissionsCalibrationStateRoot() is True


def test_constructReductionStateRoot():
    fakeIPTS = "gumdrop"
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService.generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
    runNumber = "xyz"
    ans = localDataService._constructReductionStateRoot(runNumber)
    assert isinstance(ans, Path)
    assert ans.parts[-1] == fakeState
    assert fakeIPTS in ans.parts


def test_constructReductionDataRoot():
    fakeIPTS = "gumdrop"
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService.generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
    runNumber = "xyz"
    for useLiteMode in [True, False]:
        ans = localDataService._constructReductionDataRoot(runNumber, useLiteMode)
        assert isinstance(ans, Path)
        assert ans.parts[-1] == runNumber
        assert ans.parts[-2] == "lite" if useLiteMode else "native"
        assert ans.parts[:-2] == localDataService._constructReductionStateRoot(runNumber).parts


### TESTS OF VERSIONING / INDEX METHODS ###


def test_statePathForWorkflow_calibration():
    indexerType = IndexerType.CALIBRATION
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)
        exp = localDataService._constructCalibrationStatePath(fakeStateId, useLiteMode)
        assert ans == exp


def test_statePathForWorkflow_normalization():
    indexerType = IndexerType.NORMALIZATION
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        ans = localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)
        exp = localDataService._constructNormalizationStatePath(fakeStateId, useLiteMode)
        assert ans == exp


def test_statePathForWorkflow_reduction():
    indexerType = IndexerType.REDUCTION
    runNumber = "xyz"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(runNumber, useLiteMode, indexerType)


def test_statePathForWorkflow_default():
    indexerType = IndexerType.DEFAULT
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)


def test_statePathForWorkflow_nonsense():
    indexerType = "chumbawumba"
    fakeStateId = "boogersoup"
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService._statePathForWorkflow(fakeStateId, useLiteMode, indexerType)


@mock.patch("snapred.backend.data.LocalDataService.Indexer")
def test_index_default(Indexer):
    indexer = Indexer.model_construct()
    Indexer.return_value = indexer
    localDataService = LocalDataService()

    # assert there are no indexers
    localDataService._indexer.cache_clear()
    assert localDataService._indexer.cache_info() == functools._CacheInfo(hits=0, misses=0, maxsize=128, currsize=0)

    # make an indexer
    stateId = "abc"
    localDataService.generateStateId = mock.Mock(return_value=(stateId, "gibberish"))
    localDataService._statePathForWorkflow = mock.Mock(return_value="/not/real/path")
    indexerType = IndexerType.DEFAULT
    for useLiteMode in [True, False]:
        res = localDataService.indexer("xyz", useLiteMode, indexerType)
        assert res == indexer
        assert localDataService.generateStateId.called
        assert localDataService._statePathForWorkflow.called
        assert Indexer.called

        # reset call counts for next check
        Indexer.reset_mock()
        localDataService.generateStateId.reset_mock()
        localDataService._statePathForWorkflow.reset_mock()

        # call again and make sure cached version in returned
        res = localDataService.indexer("xyz", useLiteMode, indexerType)
        assert res == indexer
        assert localDataService.generateStateId.called
        assert not localDataService._statePathForWorkflow.called
        assert not Indexer.called


@mock.patch("snapred.backend.data.LocalDataService.Indexer")
def test_index_reduction(Indexer):
    # Reduction Indexers are not supported by LocalDataService
    indexer = Indexer.model_construct()
    Indexer.return_value = indexer
    # TODO check the indexer points inside correct file
    localDataService = LocalDataService()

    # make an indexer
    localDataService.generateStateId = mock.Mock(return_value=("xyz", "123"))

    indexerType = IndexerType.REDUCTION
    for useLiteMode in [True, False]:
        with pytest.raises(NotImplementedError):
            localDataService.indexer("xyz", useLiteMode, indexerType)


def test_calibrationIndexer():
    do_test_workflow_indexer("Calibration")


def test_normalizationIndexer():
    do_test_workflow_indexer("Normalization")


def test_readCalibrationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Calibration")


def test_readNormalizationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Normalization")


def test_readWriteCalibrationIndexEntry():
    entry = IndexEntry(
        runNumber="57514",
        useLiteMode=True,
        comments="test comment",
        author="test author",
        version=randint(2, 120),
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeCalibrationIndexEntry(entry)
        actualEntries = localDataService.readCalibrationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readWriteNormalizationIndexEntry():
    entry = IndexEntry(
        runNumber="57514",
        useLiteMode=True,
        comments="test comment",
        author="test author",
        version=randint(2, 120),
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService.writeNormalizationIndexEntry(entry)
        actualEntries = localDataService.readNormalizationIndex(entry.runNumber, entry.useLiteMode)
    assert len(actualEntries) > 0
    assert actualEntries[0].runNumber == "57514"


def test_readCalibrationIndexMissing():
    do_test_index_missing("Calibration")


def test_readNormalizationIndexMissing():
    do_test_index_missing("Normalization")


### TESTS OF CALIBRATION METHODS ###


def test_createCalibrationIndexEntry():
    request = CreateIndexEntryRequest(
        runNumber="123",
        useLiteMode=True,
        version=2,
        comments="",
        author="",
        appliesTo=">=123",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createCalibrationIndexEntry(request)
        assert isinstance(ans, IndexEntry)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = VersionState.NEXT
        localDataService.calibrationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createCalibrationIndexEntry(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START


def test_createCalibrationRecord():
    record = DAOFactory.calibrationRecord("57514", True, 1)
    request = CreateCalibrationRecordRequest(**record.model_dump())
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createCalibrationRecord(request)
        assert isinstance(ans, CalibrationRecord)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = VersionState.NEXT
        localDataService.calibrationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createCalibrationRecord(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START


def test_readCalibrationRecord_with_version():
    # ensure it is calling the functionality in the indexer
    do_test_read_record_with_version("Calibration")


def test_readCalibrationRecord_no_version():
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    do_test_read_record_no_version("Calibration")


def test_writeCalibrationRecord_with_version():
    # ensure it is calling the methods inside the indexer service
    do_test_write_record_with_version("Calibration")


def test_readWriteCalibrationRecord():
    # ensure that reading and writing a calibration record will correctly interact with the file system
    # NOTE a similar test is done of the indexer, but this pre-existed and doesn't hurt to retain
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        record = DAOFactory.calibrationRecord("57514", useLiteMode, version=1)
        with state_root_redirect(localDataService):
            entry = entryFromRecord(record)
            localDataService.writeCalibrationIndexEntry(entry)
            localDataService.writeCalibrationRecord(record)
            actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode)
        assert actualRecord.version == record.version
        assert actualRecord == record


def test_writeCalibrationWorkspaces(cleanup_workspace_at_exit):
    version = randint(2, 120)
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    testCalibrationRecord = DAOFactory.calibrationRecord("57514", True, 1)
    with state_root_redirect(localDataService, stateId=stateId):
        basePath = localDataService.calibrationIndexer(testCalibrationRecord.runNumber, True).versionPath(1)

        # Workspace names need to match the names that are used in the test record.
        workspaces = testCalibrationRecord.workspaces.copy()
        runNumber = testCalibrationRecord.runNumber
        version = testCalibrationRecord.version
        outputDSPWSName = workspaces.pop(wngt.DIFFCAL_OUTPUT)[0]
        diagnosticWSName = workspaces.pop(wngt.DIFFCAL_DIAG)[0]
        tableWSName = workspaces.pop(wngt.DIFFCAL_TABLE)[0]
        maskWSName = workspaces.pop(wngt.DIFFCAL_MASK)[0]
        if workspaces:
            raise RuntimeError(f"unexpected workspace-types in record.workspaces: {workspaces}")

        # Create sample workspaces.
        CreateSampleWorkspace(
            OutputWorkspace=outputDSPWSName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0.5,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=outputDSPWSName,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        # Assign the required sample log values
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        addInstrumentLogs(outputDSPWSName, **getInstrumentLogDescriptors(detectorState1))
        assert mtd.doesExist(outputDSPWSName)
        cleanup_workspace_at_exit(outputDSPWSName)

        # Create a grouping workspace to save as the diagnostic workspace.
        ws1 = CloneWorkspace(outputDSPWSName)
        GroupWorkspaces(
            InputWorkspaces=[ws1],
            OutputWorkspace=diagnosticWSName,
        )
        assert mtd.doesExist(diagnosticWSName)
        cleanup_workspace_at_exit(diagnosticWSName)

        # Create diffraction-calibration table and mask workspaces.
        createCompatibleDiffCalTable(tableWSName, outputDSPWSName)
        createCompatibleMask(maskWSName, outputDSPWSName)
        assert mtd.doesExist(tableWSName)
        cleanup_workspace_at_exit(tableWSName)
        assert mtd.doesExist(maskWSName)
        cleanup_workspace_at_exit(maskWSName)

        localDataService.writeCalibrationWorkspaces(testCalibrationRecord)

        outputFilename = Path(outputDSPWSName + Config["calibration.diffraction.output.extension"])
        diagnosticFilename = Path(diagnosticWSName + Config["calibration.diffraction.diagnostic.extension"])
        diffCalFilename = Path(wng.diffCalTable().runNumber(runNumber).version(version).build() + ".h5")
        for filename in [outputFilename, diagnosticFilename, diffCalFilename]:
            assert (basePath / filename).exists()


### TESTS OF NORMALIZATION METHODS ###


def test_createNormalizationIndexEntry():
    request = CreateIndexEntryRequest(
        runNumber="123",
        useLiteMode=True,
        version=2,
        comments="",
        author="",
        appliesTo=">=123",
    )
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createNormalizationIndexEntry(request)
        assert isinstance(ans, IndexEntry)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = None
        localDataService.normalizationIndexer(request.runNumber, request.useLiteMode)
        ans = localDataService.createNormalizationIndexEntry(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START


def test_createNormalizationRecord():
    record = DAOFactory.normalizationRecord()
    request = CreateNormalizationRecordRequest(**record.model_dump())
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createNormalizationRecord(request)
        assert isinstance(ans, NormalizationRecord)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = VersionState.NEXT
        ans = localDataService.createNormalizationRecord(request)
        assert ans.version == VERSION_START


def test_readNormalizationRecord_with_version():
    # ensure it is calling the functionality in the indexer
    do_test_read_record_with_version("Normalization")


def test_readNormalizationRecord_no_version():
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    do_test_read_record_no_version("Normalization")


def test_writeNormalizationRecord_with_version():
    # ensure it is calling the methods inside the indexer service
    do_test_write_record_with_version("Normalization")


def test_readWriteNormalizationRecord():
    # ensure that reading and writing a normalization record will correctly interact with the file system
    record = DAOFactory.normalizationRecord()
    localDataService = LocalDataService()
    for useLiteMode in [True, False]:
        record.useLiteMode = useLiteMode
        currentVersion = randint(VERSION_START, 120)
        runNumber = record.runNumber
        record.version = currentVersion
        # NOTE redirect nested so assertion occurs outside of redirect
        # failing assertions inside tempdirs can create unwanted files
        with state_root_redirect(localDataService):
            entry = entryFromRecord(record)
            localDataService.writeNormalizationRecord(record, entry)
            indexer = localDataService.normalizationIndexer(runNumber, useLiteMode)

            indexer.index = {currentVersion: mock.MagicMock(appliesTo=runNumber, version=currentVersion)}
            actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode)
        assert actualRecord.version == record.version
        assert actualRecord.calculationParameters.version == record.calculationParameters.version
        assert actualRecord == record


def test_writeNormalizationWorkspaces(cleanup_workspace_at_exit):
    version = randint(2, 120)
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    testNormalizationRecord = DAOFactory.normalizationRecord(version=version)
    with state_root_redirect(localDataService, stateId=stateId):
        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        newWorkspaceNames = []
        for ws in testNormalizationRecord.workspaceNames:
            newWorkspaceNames.append(ws + "_" + wnvf.formatVersion(version))
        testNormalizationRecord.workspaceNames = newWorkspaceNames
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

        basePath = localDataService.normalizationIndexer(runNumber, useLiteMode).versionPath(version)

        # Create sample workspaces.
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=testWS0,
        )
        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS1)
        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS2)
        assert mtd.doesExist(testWS0)
        cleanup_workspace_at_exit(testWS0)
        assert mtd.doesExist(testWS1)
        cleanup_workspace_at_exit(testWS1)
        assert mtd.doesExist(testWS2)
        cleanup_workspace_at_exit(testWS2)

        localDataService.writeNormalizationWorkspaces(testNormalizationRecord)

        for wsName in testNormalizationRecord.workspaceNames:
            filename = Path(wsName + ".nxs")
            assert (basePath / filename).exists()


### TESTS OF REDUCTION METHODS ###


def _writeSyntheticReductionRecord(filePath: Path, timestamp: float):
    # Create a `ReductionRecord` JSON file to be used by the unit tests.

    # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
    testCalibration = DAOFactory.calibrationRecord("57514", True, 1)
    testNormalization = DAOFactory.normalizationRecord("57514", True, 2)
    testRecord = ReductionRecord(
        runNumber=testCalibration.runNumber,
        useLiteMode=testCalibration.useLiteMode,
        timestamp=timestamp,
        calibration=testCalibration,
        normalization=testNormalization,
        pixelGroupingParameters={
            pg.focusGroup.name: list(pg.pixelGroupingParameters.values()) for pg in testCalibration.pixelGroups
        },
        workspaceNames=[
            wng.reductionOutput()
            .runNumber(testCalibration.runNumber)
            .group(pg.focusGroup.name)
            .timestamp(timestamp)
            .build()
            for pg in testCalibration.pixelGroups
        ],
    )
    write_model_pretty(testRecord, filePath)


def _writeSyntheticReductionIngredients(filePath: Path):
    # Create a `ReductionIngredients` JSON file to be used by the unit tests.

    # TODO: Implement methods to create the synthetic `CalibrationRecord` and `NormalizationRecord`.
    calibration = CalibrationRecord.model_validate_json(
        Resource.read("inputs/calibration/CalibrationRecord_v0001.json")
    )
    normalization = NormalizationRecord.model_validate_json(
        Resource.read("inputs/normalization/NormalizationRecord.json")
    )

    peaksRefFile = "/outputs/predict_peaks/peaks.json"
    peaks_ref = pydantic.TypeAdapter(List[GroupPeakList]).validate_json(Resource.read(peaksRefFile))

    ingredients = ReductionIngredients(
        calibration=calibration,
        normalization=normalization,
        pixelGroups=calibration.pixelGroups,
        detectorPeaksMany=[peaks_ref, peaks_ref],
    )
    write_model_pretty(ingredients, filePath)


def test_readWriteReductionRecord_timestamps():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord(inputRecordFilePath, "1")

    with open(inputRecordFilePath, "r") as f:
        testReductionRecord_v0001 = ReductionRecord.model_validate_json(f.read())
    oldTimestamp = testReductionRecord_v0001.timestamp

    # Get a second copy, with a different timestamp
    newTimestamp = time.time()
    dict_ = testReductionRecord_v0001.model_dump()
    dict_["timestamp"] = newTimestamp
    testReductionRecord_v0002 = ReductionRecord.model_validate(
        dict_,
    )
    assert oldTimestamp != newTimestamp

    runNumber, useLiteMode = testReductionRecord_v0001.runNumber, testReductionRecord_v0001.useLiteMode
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        # write: old timestamp
        localDataService.writeReductionRecord(testReductionRecord_v0001)
        # write call should not modify timestamp
        assert testReductionRecord_v0001.timestamp == oldTimestamp
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, oldTimestamp)
        assert actualRecord.timestamp == oldTimestamp

        # write: new timestamp
        actualRecord = localDataService.writeReductionRecord(testReductionRecord_v0002)
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, newTimestamp)
        assert actualRecord.timestamp == newTimestamp


def test_readWriteReductionRecord():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    # Create the input data for this test:
    # _writeSyntheticReductionRecord("1", inputRecordFilePath)
    with open(inputRecordFilePath, "r") as f:
        testRecord = ReductionRecord.model_validate_json(f.read())

    runNumber = testRecord.runNumber
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService._getLatestReductionVersionNumber = mock.Mock(return_value=0)
        localDataService.groceryService = mock.Mock()
        localDataService.writeReductionRecord(testRecord)
        actualRecord = localDataService.readReductionRecord(runNumber, testRecord.useLiteMode, testRecord.timestamp)
    assert actualRecord.dict() == testRecord.dict()


@pytest.fixture
def readSyntheticReductionRecord():
    # Read a `ReductionRecord` from the specified file path:
    #   * update the record's timestamp and workspace-name list using the specified value.

    def _readSyntheticReductionRecord(filePath: Path, timestamp: float) -> ReductionRecord:
        with open(filePath, "r") as f:
            record = ReductionRecord.model_validate_json(f.read())

        # Update the record's list of workspace names:
        #   * reconstruct complete `WorkspaceName` with builder.
        #   * ensure that the names are unique to this test by using the new timestap => enables parallel testing;

        wngReducedOutput = re.compile(
            r"_reduced_([A-Za-z]+)_([A-Za-z]+)_([0-9]{6,})_([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})"
        )
        wngReductionPixelMask = re.compile(
            r"_pixelmask_([0-9]{6,})_([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})"
        )

        wss: List[WorkspaceName] = []
        for ws in record.workspaceNames:
            wngOutput = wngReducedOutput.match(ws)
            if wngOutput:
                unit, grouping, runNumber = wngOutput.group(1), wngOutput.group(2), wngOutput.group(3)
                ws_ = wng.reductionOutput().unit(unit).group(grouping).runNumber(runNumber).timestamp(timestamp).build()
                wss.append(ws_)
                continue
            wngPixelMask = wngReductionPixelMask.match(ws)
            if wngPixelMask:
                runNumber = wngPixelMask.group(1)
                ws_ = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
                wss.append(ws_)
                continue
            raise RuntimeError(f"unable to reconstruct 'WorkspaceName' from '{ws}'")

        # Reconstruct the record, in order to modify the frozen fields
        dict_ = record.model_dump()
        dict_["timestamp"] = timestamp
        #    WARNING: we cannot just use `model_validate` here,
        #      it will recreate the `WorkspaceName(<original name>)` and
        #        the `_builder` args will be stripped.
        #   (TODO: is this still correct?  I think it works now.)
        record = ReductionRecord.model_validate(dict_)
        record.workspaceNames = wss

        return record

    yield _readSyntheticReductionRecord

    # teardown...
    pass


@pytest.fixture
def createReductionWorkspaces(cleanup_workspace_at_exit):
    # Create sample workspaces from a list of names:
    #   * these workspaces are automatically deleted at teardown.

    def _createWorkspaces(wss: List[WorkspaceName]):
        # Create several sample reduction event workspaces with DSP units
        src = mtd.unique_hidden_name()
        CreateSampleWorkspace(
            OutputWorkspace=src,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=0,
            BankPixelWidth=4,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0,
            XMax=8000,
            BinWidth=100,
        )
        LoadInstrument(
            Workspace=src,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )

        # Mask workspace uses legacy instrument
        mask = mtd.unique_hidden_name()
        createCompatibleMask(mask, src)

        if Config["reduction.output.useEffectiveInstrument"]:
            # Convert the source workspace's instrument to the reduced form:
            #   * no monitors;
            #   * only one bank of detectors;
            #   * no parameter map.

            detectorInfo = mtd[src].detectorInfo()
            l2s, twoThetas, azimuths = [], [], []
            for n in range(detectorInfo.size()):
                if detectorInfo.isMonitor(n):
                    continue

                l2 = detectorInfo.l2(n)
                twoTheta = detectorInfo.twoTheta(n)

                # See: defect EWM#7384
                try:
                    azimuth = detectorInfo.azimuthal(n)
                except RuntimeError as e:
                    if not str(e).startswith("Failed to create up axis"):
                        raise
                    azimuth = 0.0
                l2s.append(l2)
                twoThetas.append(twoTheta)
                azimuths.append(azimuth)

            EditInstrumentGeometry(Workspace=src, L2=l2s, Polar=np.rad2deg(twoThetas), Azimuthal=np.rad2deg(azimuths))
        assert mtd.doesExist(src)

        for ws in wss:
            CloneWorkspace(
                OutputWorkspace=ws,
                InputWorkspace=src if ws.tokens("workspaceType") != wngt.REDUCTION_PIXEL_MASK else mask,
            )
            assert mtd.doesExist(ws)
            cleanup_workspace_at_exit(ws)

        DeleteWorkspaces([src, mask])
        return wss

    yield _createWorkspaces

    # teardown
    pass


def test_writeReductionData(readSyntheticReductionRecord, createReductionWorkspaces):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718908813.0250459
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    # Temporarily use a single run number
    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionFilePath.exists()

        # `writeReductionRecord` must be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        assert reductionFilePath.exists()


def test_writeReductionData_legacy_instrument(readSyntheticReductionRecord, createReductionWorkspaces):
    # Test that the special `Config` setting allows the saving of workspaces with non-reduced instruments

    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1731518208.172797
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    # Temporarily use a single run number
    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    with Config_override("reduction.output.useEffectiveInstrument", False):
        wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
        localDataService = LocalDataService()
        with reduction_root_redirect(localDataService, stateId=stateId):
            localDataService.instrumentConfig = mock.Mock()
            localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

            # Important to this test: use a path that doesn't already exist
            reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
            assert not reductionFilePath.exists()

            # `writeReductionRecord` must be called first
            localDataService.writeReductionRecord(testRecord)
            localDataService.writeReductionData(testRecord)

            assert reductionFilePath.exists()


def test_writeReductionData_effective_instrument(readSyntheticReductionRecord, createReductionWorkspaces):
    # Test that the special `Config` setting allows the saving of workspaces with effective instruments

    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1733189687.0684218
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    # Temporarily use a single run number
    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    with Config_override("reduction.output.useEffectiveInstrument", True):
        wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
        localDataService = LocalDataService()
        with reduction_root_redirect(localDataService, stateId=stateId):
            localDataService.instrumentConfig = mock.Mock()
            localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

            # Important to this test: use a path that doesn't already exist
            reductionFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
            assert not reductionFilePath.exists()

            # `writeReductionRecord` must be called first
            localDataService.writeReductionRecord(testRecord)
            localDataService.writeReductionData(testRecord)

            assert reductionFilePath.exists()


def test_writeReductionData_no_directories(readSyntheticReductionRecord, createReductionWorkspaces):  # noqa: ARG001
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718908816.106522
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["nexus.file.extension"]

    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` must be called first
        # * deliberately _not_ done in this test => <reduction-data root> directory won't exist
        with pytest.raises(RuntimeError) as einfo:
            localDataService.writeReductionData(testRecord)
        msg = str(einfo.value)
    assert "reduction version directories" in msg
    assert "do not exist" in msg


def test_writeReductionData_metadata(readSyntheticReductionRecord, createReductionWorkspaces):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909723.027197
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` must be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()
        with h5py.File(filePath, "r") as h5:
            dict_ = n5m.extractMetadataGroup(h5, "/metadata")
            actualRecord = ReductionRecord.model_validate(dict_)
            assert actualRecord == testRecord


def test_readWriteReductionData(readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909801.91552
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` needs to be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()

        # move the existing test workspaces out of the way:
        #   * this just adds the `_uniquePrefix` one more time.
        RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
        # append to the cleanup list
        for ws in wss:
            cleanup_workspace_at_exit(_uniquePrefix + ws)

        actualRecord = localDataService.readReductionData(runNumber, useLiteMode, timestamp)

        assert actualRecord.normalization.calibrationVersionUsed == testRecord.normalization.calibrationVersionUsed
        assert actualRecord.dict() == testRecord.dict()

        # workspaces should have been reloaded with their original names
        # Implementation note:
        #   * the workspaces must match _exactly_ here, so `CompareWorkspaces` must be used;
        #   please do _not_ replace this with one of the `assert_almost_equal` methods:
        #   -- they do not necessarily do what you think they should do...
        for ws in actualRecord.workspaceNames:
            equal, _ = CompareWorkspaces(Workspace1=ws, Workspace2=_uniquePrefix + ws, CheckAllData=True)
            assert equal


def test_readWriteReductionData_legacy_instrument(
    readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit
):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1731519071.6706867
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    with Config_override("reduction.output.useEffectiveInstrument", False):
        wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
        localDataService = LocalDataService()
        with reduction_root_redirect(localDataService, stateId=stateId):
            localDataService.instrumentConfig = mock.Mock()
            localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

            # Important to this test: use a path that doesn't already exist
            reductionRecordFilePath = localDataService._constructReductionRecordFilePath(
                runNumber, useLiteMode, timestamp
            )
            assert not reductionRecordFilePath.exists()

            # `writeReductionRecord` needs to be called first
            localDataService.writeReductionRecord(testRecord)
            localDataService.writeReductionData(testRecord)

            filePath = reductionRecordFilePath.parent / fileName
            assert filePath.exists()

            # move the existing test workspaces out of the way:
            #   * this just adds the `_uniquePrefix` one more time.
            RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
            # append to the cleanup list
            for ws in wss:
                cleanup_workspace_at_exit(_uniquePrefix + ws)

            actualRecord = localDataService.readReductionData(runNumber, useLiteMode, timestamp)
            assert actualRecord == testRecord

            # workspaces should have been reloaded with their original names
            # Implementation note:
            #   * the workspaces must match _exactly_ here, so `CompareWorkspaces` must be used;
            #   please do _not_ replace this with one of the `assert_almost_equal` methods:
            #   -- they do not necessarily do what you think they should do...
            for ws in actualRecord.workspaceNames:
                equal, _ = CompareWorkspaces(Workspace1=ws, Workspace2=_uniquePrefix + ws, CheckAllData=True)
                assert equal


def test_readWriteReductionData_pixel_mask(
    readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit
):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_PM_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909911.6432922
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]
    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value="IPTS-12345")

        # Important to this test: use a path that doesn't already exist
        reductionRecordFilePath = localDataService._constructReductionRecordFilePath(runNumber, useLiteMode, timestamp)
        assert not reductionRecordFilePath.exists()

        # `writeReductionRecord` needs to be called first
        localDataService.writeReductionRecord(testRecord)
        localDataService.writeReductionData(testRecord)

        filePath = reductionRecordFilePath.parent / fileName
        assert filePath.exists()

        # 1) Verify that a pixel mask was separately written in `SaveDiffCal` format
        maskName = wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()
        assert (reductionRecordFilePath.parent / (maskName + ".h5")).exists()

        # move the existing test workspaces out of the way:
        #   * this just adds the `_uniquePrefix`.
        RenameWorkspaces(InputWorkspaces=wss, Prefix=_uniquePrefix)
        # append to the cleanup list
        for ws in wss:
            cleanup_workspace_at_exit(_uniquePrefix + ws)

        actualRecord = localDataService.readReductionData(runNumber, useLiteMode, timestamp)
        # 2) Verify that a pixel mask was appended to the 'workspaceNames' list
        assert maskName in actualRecord.workspaceNames

        # 3) Verify that the pixel mask has been appended to the combined data file,
        #      and that it is reloaded as a `MaskWorkspace` instance
        pixelMaskKeyword = Config["mantid.workspace.nameTemplate.template.reduction.pixelMask"].split(",")[0]
        # verify that a pixel mask was appended to the
        maskIsAppendedToData = False
        for ws in actualRecord.workspaceNames:
            if pixelMaskKeyword in ws:
                if isinstance(mtd[ws], MaskWorkspace):
                    maskIsAppendedToData = True
        assert maskIsAppendedToData


def test__constructReductionDataFilePath():
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    with open(inputRecordFilePath, "r") as f:
        testRecord = ReductionRecord.model_validate_json(f.read())

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    testIPTS = "IPTS-12345"
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    expectedFilePath = (
        Path(Config["instrument.reduction.home"].format(IPTS=testIPTS))
        / stateId
        / ("lite" if useLiteMode else "native")
        / runNumber
        / wnvf.pathTimestamp(timestamp)
        / fileName
    )

    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=(stateId, None))
    localDataService.getIPTS = mock.Mock(return_value=testIPTS)
    actualFilePath = localDataService._constructReductionDataFilePath(runNumber, useLiteMode, timestamp)
    assert actualFilePath == expectedFilePath


def test_getReductionRecordFilePath():
    timestamp = time.time()
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock()
    localDataService.generateStateId.return_value = ("123", "456")
    localDataService._constructReductionDataRoot = mock.Mock()
    localDataService._constructReductionDataRoot.return_value = Path(Resource.getPath("outputs"))
    actualPath = localDataService._constructReductionRecordFilePath("57514", True, timestamp)
    assert actualPath == Path(Resource.getPath("outputs")) / wnvf.pathTimestamp(timestamp) / "ReductionRecord.json"


# end interlude #


### TESTS OF READ / WRITE STATE METHODS ###


def test_readCalibrationState_with_version():
    do_test_read_state_with_version("Calibration")


def test_readCalibrationState_no_version():
    do_test_read_state_no_version("Calibration")


def test_readNormalizationState_with_version():
    do_test_read_state_with_version("Normalization")


def test_readNormalizationState_no_version():
    do_test_read_state_no_version("Normalization")


def test_readWriteCalibrationState():
    runNumber = "123"
    localDataService = LocalDataService()
    mockCalibrationIndexer = mock.Mock()

    localDataService.calibrationIndexer = mock.Mock(return_value=mockCalibrationIndexer)
    localDataService.calibrationIndexer().latestApplicableVersion = mock.Mock(return_value=1)
    mockCalibrationIndexer.nextVersion = mock.Mock(return_value=1)

    ans = localDataService.readCalibrationState(runNumber, True, VersionState.LATEST)
    assert ans == mockCalibrationIndexer.readParameters.return_value
    mockCalibrationIndexer.readParameters.assert_called_once_with(1)


def test_readWriteCalibrationState_noWritePermissions():
    localDataService = LocalDataService()
    localDataService.calibrationExists = mock.Mock(return_value=False)
    localDataService._hasWritePermissionsCalibrationStateRoot = mock.Mock(return_value=False)

    with pytest.raises(
        RuntimeError,
        match=r".*No calibration exists, and you lack permissions to create one. Please contact your IS or CIS.*",
    ):
        localDataService.readCalibrationState("123", True)


def test_readCalibrationState_hasWritePermissions():
    localDataService = LocalDataService()
    localDataService.calibrationExists = mock.Mock(return_value=False)
    localDataService._hasWritePermissionsCalibrationStateRoot = mock.Mock(return_value=True)

    with pytest.raises(RecoverableException, match="State uninitialized"):
        localDataService.readCalibrationState("123", True)


@mock.patch("snapred.backend.data.GroceryService.GroceryService.createDiffcalTableWorkspaceName")
@mock.patch("snapred.backend.data.GroceryService.GroceryService._fetchInstrumentDonor")
def test_writeDefaultDiffCalTable(fetchInstrumentDonor, createDiffCalTableWorkspaceName):
    # verify that the default diffcal table is being written to the default state directory
    runNumber = "default"
    version = VERSION_START
    useLiteMode = True
    # mock the grocery service to return the fake instrument to use for geometry
    idfWS = mtd.unique_name(prefix="_idf_")
    LoadEmptyInstrument(
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
        OutputWorkspace=idfWS,
    )
    fetchInstrumentDonor.return_value = idfWS
    # mock the file names to check them later
    wsName = f"diffcal_{runNumber}_{wnvf.formatVersion(version)}"
    createDiffCalTableWorkspaceName.return_value = wsName
    # now write the diffcal file
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        localDataService._writeDefaultDiffCalTable(runNumber, useLiteMode)
        file = localDataService.calibrationIndexer(runNumber, useLiteMode).versionPath(version) / wsName
        file = file.with_suffix(".h5")
        assert file.exists()


def test_readWriteNormalizationState():
    # NOTE this test is already covered by tests of the Indexer
    # but it doesn't hurt to retain this test anyway
    runNumber = "123"
    localDataService = LocalDataService()
    mockNormalizationIndexer = mock.Mock()

    localDataService.normalizationIndexer = mock.Mock(return_value=mockNormalizationIndexer)
    localDataService.normalizationIndexer().latestApplicableVersion = mock.Mock(return_value=1)
    mockNormalizationIndexer.nextVersion = mock.Mock(return_value=1)

    ans = localDataService.readNormalizationState(runNumber, True, VersionState.LATEST)
    assert ans == mockNormalizationIndexer.readParameters.return_value
    mockNormalizationIndexer.readParameters.assert_called_once_with(1)


def test_readDetectorState(monkeypatch):
    mockMappingFromNeXusLogs = mock.Mock(return_value=mock.Mock())
    monkeypatch.setattr(LocalDataServiceModule, "mappingFromNeXusLogs", mockMappingFromNeXusLogs)
    instance = LocalDataService()
    expected = mockDetectorState("123")
    instance._detectorStateFromMapping = mock.Mock(return_value=expected)
    instance._readPVFile = mock.Mock(return_value=mock.Mock())
    instance._constructPVFilePath = mock.Mock(return_value="/mock/path")
    actual = instance.readDetectorState("123")
    assert actual == expected
    mockMappingFromNeXusLogs.assert_called_once_with(instance._readPVFile.return_value)
    instance._detectorStateFromMapping.assert_called_once_with(mockMappingFromNeXusLogs.return_value)


def test_readDetectorState_no_PVFile():
    runNumber = "123"
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("lah dee dah"))
    with pytest.raises(FileNotFoundError, match="lah dee dah"):
        instance.readDetectorState(runNumber)


def test_readDetectorState_no_PVFile_no_connection():
    runNumber = "12345"
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("lah dee dah"))
    instance.hasLiveDataConnection = mock.Mock(return_value=False)
    with pytest.raises(FileNotFoundError, match="lah dee dah"):
        instance.readDetectorState(runNumber)


def test_readDetectorState_live_run():
    runNumber = "123"
    expected = mockDetectorState(runNumber)
    mockLiveMetadata = mock.Mock(spec=LiveMetadata, runNumber=runNumber, detectorState=expected, protonCharge=1000.0)

    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("lah dee dah"))
    instance.hasLiveDataConnection = mock.Mock(return_value=True)
    instance.readLiveMetadata = mock.Mock(return_value=mockLiveMetadata)

    actual = instance.readDetectorState(runNumber)
    assert actual == expected

    instance._readPVFile.assert_called_once_with(runNumber)
    instance.hasLiveDataConnection.assert_called_once()
    instance.readLiveMetadata.assert_called_once()


def test_readDetectorState_live_run_mismatch():
    # Test that an unexpected live run number throws an exception.
    runNumber0 = "12345"
    runNumber1 = "67890"
    mockLiveMetadata = mock.Mock(
        spec=LiveMetadata, runNumber=runNumber1, detectorState=mockDetectorState(runNumber1), protonCharge=1000.0
    )

    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("lah dee dah"))
    instance.hasLiveDataConnection = mock.Mock(return_value=True)
    instance.readLiveMetadata = mock.Mock(return_value=mockLiveMetadata)

    with pytest.raises(RuntimeError, match=f"No PVFile exists for run {runNumber0}, and it isn't a live run."):
        instance.readDetectorState(runNumber0)


def test__detectorStateFromMapping():
    localDataService = LocalDataService()
    expected = mockDetectorState("123")
    logs = mockNeXusLogsMapping(expected)
    actual = localDataService._detectorStateFromMapping(logs)
    assert actual == expected


def test__detectorStateFromMappingWithDiffWavKey():
    localDataService = LocalDataService()
    logsDict = {
        "BL3:Chop:Gbl:WavelengthReq": [1.1],
        "det_arc1": [1],
        "det_arc2": [2],
        "BL3:Det:TH:BL:Frequency": [1.2],
        "BL3:Mot:OpticsPos:Pos": [1],
        "det_lin1": [1],
        "det_lin2": [2],
    }
    mockLogs = mockNeXusLogsMappingFromDict(logsDict)

    expected = DetectorState(
        arc=[1, 2],
        wav=1.1,
        freq=1.2,
        guideStat=1,
        lin=[1, 2],
    )
    actual = localDataService._detectorStateFromMapping(mockLogs)

    assert expected == actual


def test__detectorStateFromMapping_bad_logs():
    localDataService = LocalDataService()
    testDetectorState = mockDetectorState("12345")

    # Case where wavelength logs are missing
    logs_missing_wav = mockNeXusLogsMapping(testDetectorState)
    logs_missing_wav.del_item("BL3:Chop:Skf1:WavelengthUserReq")

    with pytest.raises(RuntimeError, match=r"Some required logs are not present.*"):
        localDataService._detectorStateFromMapping(logs_missing_wav)

    # Case where other required logs are missing
    missing_logs = {
        "BL3:Chop:Skf1:WavelengthUserReq": [1.1]
        # Other required logs are missing
    }
    mock_missing_logs = mockNeXusLogsMappingFromDict(missing_logs)

    with pytest.raises(RuntimeError, match=r"Some required logs are not present.*"):
        localDataService._detectorStateFromMapping(mock_missing_logs)

    # Case where value in wavelength logs is not valid
    invalid_wav_dict = {
        "BL3:Chop:Gbl:WavelengthReq": "glitch",
        "det_arc1": [1],
        "det_arc2": [2],
        "BL3:Det:TH:BL:Frequency": [1.2],
        "BL3:Mot:OpticsPos:Pos": [1],
        "det_lin1": [1],
        "det_lin2": [2],
    }
    invalid_wav = mockNeXusLogsMappingFromDict(invalid_wav_dict)

    with pytest.raises(RuntimeError, match=r"Logs have an unexpected format.*"):
        localDataService._detectorStateFromMapping(invalid_wav)


@mock.patch(ThisService + "mappingFromRun")
def test_liveMetadataFromRun(mockMapping):
    logs = mockNeXusLogsMapping(DAOFactory.real_detector_state)
    mockMapping.return_value = logs
    mockRun = mock.Mock(spec=Run)
    instance = LocalDataService()
    expected = LiveMetadata(
        runNumber=logs["run_number"],
        startTime=datetime.datetime.replace(
            np.datetime64(logs["start_time"], "us").astype(datetime.datetime), tzinfo=datetime.timezone.utc
        ),
        endTime=datetime.datetime.replace(
            np.datetime64(logs["end_time"], "us").astype(datetime.datetime), tzinfo=datetime.timezone.utc
        ),
        detectorState=DAOFactory.real_detector_state,
        protonCharge=1000.0,
    )
    actual = instance._liveMetadataFromRun(mockRun)

    assert actual == expected
    mockMapping.assert_called_once_with(mockRun)


@mock.patch(ThisService + "mappingFromRun")
def test_liveMetadataFromRun_incomplete_logs(mockMapping):
    logs = mockNeXusLogsMapping(DAOFactory.real_detector_state)
    logs.del_item("run_number")
    mockMapping.return_value = logs
    mockRun = mock.Mock(spec=Run)
    instance = LocalDataService()
    with pytest.raises(RuntimeError, match=r"unable to extract LiveMetadata from Run.*"):
        instance._liveMetadataFromRun(mockRun)


@mock.patch(ThisService + "MantidSnapper")
def test__readLiveData(mockSnapper):
    now_ = datetime.datetime.utcnow()
    duration = 42
    # 42 seconds ago
    now_minus_42 = now_ + datetime.timedelta(seconds=-duration)
    expectedStartTime = now_minus_42.isoformat()
    testWs = "testWs"

    with mock.patch(ThisService + "datetime.datetime", wraps=datetime.datetime) as mockDatetime:
        mockDatetime.utcnow.return_value = now_

        instance = LocalDataService()
        result = instance._readLiveData(testWs, duration)
        mockSnapper.return_value.LoadLiveData.assert_called_with(
            "load live-data chunk",
            OutputWorkspace=testWs,
            Instrument=Config["liveData.instrument.name"],
            AccumulationMethod=Config["liveData.accumulationMethod"],
            StartTime=expectedStartTime,
        )
        mockSnapper.return_value.executeQueue.assert_called_once()
        assert result == testWs


@mock.patch(ThisService + "MantidSnapper")
def test__readLiveData_all_data(mockSnapper):
    now_ = datetime.datetime.utcnow()
    duration = 0
    # _zero_ is special case => "": <StartTime: default value> => read all-available data
    expectedStartTime = ""
    testWs = "testWs"

    with mock.patch(ThisService + "datetime.datetime", wraps=datetime.datetime) as mockDatetime:
        mockDatetime.utcnow.return_value = now_

        instance = LocalDataService()
        result = instance._readLiveData(testWs, duration)  # noqa: F841
        mockSnapper.return_value.LoadLiveData.assert_called_with(
            "load live-data chunk",
            OutputWorkspace=testWs,
            Instrument=Config["liveData.instrument.name"],
            AccumulationMethod=Config["liveData.accumulationMethod"],
            StartTime=expectedStartTime,
        )


@mock.patch(ThisService + "MantidSnapper")
def test_readLiveMetadata(mockSnapper):
    testWs = "testWs"
    mockWs = mock.Mock()
    mockWs.getRun.return_value = mock.sentinel.run

    mockSnapper.return_value.mtd.unique_hidden_name.return_value = testWs
    mockSnapper.return_value.mtd.__getitem__.return_value = mockWs

    instance = LocalDataService()
    with (
        mock.patch.object(instance, "_readLiveData") as mock__readLiveData,
        mock.patch.object(instance, "_liveMetadataFromRun") as mock__liveMetadataFromRun,
    ):
        mock__readLiveData.return_value = testWs
        mock__liveMetadataFromRun.return_value = mock.sentinel.metadata
        actual = instance.readLiveMetadata()
        mock__readLiveData.assert_called_once_with(testWs, duration=1)
        mock__liveMetadataFromRun.assert_called_once_with(mock.sentinel.run)
        mockSnapper.return_value.DeleteWorkspace.assert_called_once_with("delete temporary workspace", Workspace=testWs)
        mockSnapper.return_value.executeQueue.call_count == 2
        assert actual == mock.sentinel.metadata


def test_readLiveData():
    testWs = "testWs"
    duration = 42
    instance = LocalDataService()
    with mock.patch.object(instance, "_readLiveData") as mock__readLiveData:
        instance.readLiveData(testWs, duration)
        mock__readLiveData.assert_called_once_with(testWs, duration)


@pytest.fixture
def instrumentWorkspace(cleanup_workspace_at_exit):
    useLiteMode = True
    wsName = mtd.unique_hidden_name()

    # Load the bare instrument:
    instrumentFilename = (
        Config["instrument.lite.definition.file"] if useLiteMode else Config["instrument.native.definition.file"]
    )
    LoadEmptyInstrument(
        Filename=instrumentFilename,
        OutputWorkspace=wsName,
    )
    cleanup_workspace_at_exit(wsName)
    yield wsName

    # teardown...
    pass


def test_detectorStateFromWorkspace(instrumentWorkspace):
    service = LocalDataService()
    detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    wsName = instrumentWorkspace

    # --- duplicates `groceryService.updateInstrumentParameters`: -----
    logsInfo = getInstrumentLogDescriptors(detectorState1)
    addInstrumentLogs(wsName, **logsInfo)
    # ------------------------------------------------------

    actual = service.detectorStateFromWorkspace(wsName)
    assert actual == detectorState1


def test_detectorStateFromWorkspaceWithDiffWavKey(instrumentWorkspace):
    service = LocalDataService()
    detectorState2 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    wsName = instrumentWorkspace

    # --- duplicates `groceryService.updateInstrumentParameters`: -----
    logsInfo = getInstrumentLogDescriptors(detectorState2)
    idx = logsInfo["logNames"].index("BL3:Chop:Skf1:WavelengthUserReq")
    logsInfo["logNames"][idx] = "BL3:Chop:Gbl:WavelengthReq"
    addInstrumentLogs(wsName, **logsInfo)
    # ------------------------------------------------------

    actual = service.detectorStateFromWorkspace(wsName)
    assert actual == detectorState2


def test_detectorStateFromWorkspace_bad_logs(instrumentWorkspace):
    service = LocalDataService()
    detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    wsName = instrumentWorkspace

    # --- duplicates `groceryService.updateInstrumentParameters`: but skips a few log entries: -----
    logsInfo = {
        "logNames": [
            "det_arc1",
            "det_arc2",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ],
        "logTypes": [
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
            "Number Series",
        ],
        "logValues": [
            str(detectorState1.arc[0]),
            str(detectorState1.arc[1]),
            str(detectorState1.guideStat),
            str(detectorState1.lin[0]),
            str(detectorState1.lin[1]),
        ],
    }
    addInstrumentLogs(wsName, **logsInfo)
    # ------------------------------------------------------

    with pytest.raises(RuntimeError, match=r"Some required logs are not present.*"):
        service.detectorStateFromWorkspace(wsName)


def test_stateIdFromWorkspace(instrumentWorkspace):
    service = LocalDataService()
    detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    wsName = instrumentWorkspace

    # --- duplicates `groceryService.updateInstrumentParameters`: -----
    logsInfo = getInstrumentLogDescriptors(detectorState1)
    addInstrumentLogs(wsName, **logsInfo)
    # ------------------------------------------------------

    SHA = service._stateIdFromDetectorState(detectorState1)
    expected = SHA.hex, detectorState1
    actual = service.stateIdFromWorkspace(wsName)
    assert actual == expected


def test_initializeState():
    # Test 'initializeState'; test basic functionality.
    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    testDetectorState = mockDetectorState(runNumber)
    localDataService._readPVFile.return_value = mockPVFile(testDetectorState)
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = DAOFactory.calibrationParameters(
        runNumber=runNumber,
        useLiteMode=useLiteMode,
        version=VERSION_START,
        instrumentState=DAOFactory.pv_instrument_state.copy(),
    )

    localDataService._instrumentConfig = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._prepareStateRoot = mock.Mock()

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = testCalibrationData.instrumentState.id.hex  # "ab8704b0bc2a2342"
        stateRootPath = Path(tmpDir) / stateId
        localDataService.constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

        actual = localDataService.initializeState(runNumber, useLiteMode, "test")
        actual.creationDate = testCalibrationData.creationDate

    assert actual == testCalibrationData
    assert localDataService._writeDefaultDiffCalTable.call_count == 2
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, True)
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, False)


def test_initializeState_calls_prepareStateRoot():
    # Test that 'initializeState' initializes the <state root> directory.

    runNumber = "123"
    useLiteMode = True

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock()
    testDetectorState = mockDetectorState("12345")
    localDataService._readPVFile.return_value = mockPVFile(testDetectorState)
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = DAOFactory.calibrationParameters()
    localDataService._instrumentConfig = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(return_value=mock.Mock(isDirty=False))

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = ENDURING_STATE_ID
        stateRootPath = Path(tmpDir) / stateId
        localDataService.constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

        assert not stateRootPath.exists()
        localDataService.initializeState(runNumber, useLiteMode, "test")
        assert stateRootPath.exists()


# NOTE: This test fails on analysis because the instrument home actually does exist!
@pytest.mark.skipif(
    IS_ON_ANALYSIS_MACHINE, reason="This test fails on analysis because the instrument home actually does exist!"
)
def test_badPaths():
    """This verifies that a broken configuration (from production) can't find all of the files"""
    # get a handle on the service
    service = LocalDataService()
    service.verifyPaths = True  # override test setting
    with Config_override("instrument.home", "this/path/does/not/exist"):
        with pytest.raises(FileNotFoundError):
            service._readInstrumentConfig()
    service.verifyPaths = False  # put the setting back


def test_noInstrumentConfig():
    """This verifies that a broken configuration (from production) can't find all of the files"""
    # get a handle on the service
    service = LocalDataService()
    service.verifyPaths = True  # override test setting
    with Config_override("instrument.config", "this/path/does/not/exist"):
        with pytest.raises(FileNotFoundError):
            service._readInstrumentConfig()
    service.verifyPaths = False  # put the setting back


# interlude -- a missplaced calibrant sample method test #


def test_readSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = [
        "/sample1.json",
        "/sample2.json",
    ]
    result = localDataService.readSampleFilePaths()
    assert len(result) == 2
    assert "/sample1.json" in result
    assert "/sample2.json" in result


# end interlude


##### TESTS OF GROUPING MAP METHODS #####


def test_readGroupingMap_no_calibration_file():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=False)
    localDataService.generateStateId = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))
    localDataService._readDefaultGroupingMap = mock.Mock()
    localDataService._readGroupingMap = mock.Mock()

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)  # noqa: F841
    assert localDataService._readDefaultGroupingMap.called


def test_readGroupingMap_yes_calibration_file():
    localDataService = LocalDataService()
    localDataService.checkCalibrationFileExists = mock.Mock(return_value=True)
    localDataService.generateStateId = mock.Mock(return_value=(mock.Mock(), mock.Mock()))
    localDataService._readGroupingMap = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(side_effect=RuntimeError("YOU IDIOT!"))

    runNumber = "flan"
    res = localDataService.readGroupingMap(runNumber)  # noqa: F841
    assert not localDataService._readDefaultGroupingMap.called


##### TESTS OF CALIBRANT SAMPLE METHODS #####


def test_readNoSampleFilePaths():
    localDataService = LocalDataService()
    localDataService._findMatchingFileList = mock.Mock()
    localDataService._findMatchingFileList.return_value = []

    with pytest.raises(RuntimeError) as e:
        localDataService.readSampleFilePaths()
    assert "No samples found" in str(e.value)


# interlude -- missplaced tests of grouping map #


def test__readDefaultGroupingMap():
    service = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(service, stateId=stateId) as tmpRoot:
        groupingMap = DAOFactory.groupingMap_SNAP(GroupingMap.defaultStateId)
        with Config_override("instrument.calibration.powder.grouping.home", tmpRoot.path().parent):
            tmpRoot.saveObjectAt(groupingMap, service._defaultGroupingMapPath())
            groupingMap = service._readDefaultGroupingMap()
    assert groupingMap.isDefault


def test_readGroupingMap_default_not_found():
    service = LocalDataService()
    with Config_override("instrument.calibration.powder.grouping.home", Resource.getPath("inputs/")):
        with pytest.raises(FileNotFoundError, match="default grouping-schema map"):
            service._readDefaultGroupingMap()


def test_readGroupingMap_initialized_state():
    # Test that '_readGroupingMap' for an initialized state returns the state's <grouping map>.
    service = LocalDataService()
    stateId = ENDURING_STATE_ID
    with state_root_redirect(service, stateId=stateId) as tmpRoot:
        tmpRoot.saveObjectAt(DAOFactory.groupingMap_SNAP(stateId), service._groupingMapPath(stateId))
        groupingMap = service._readGroupingMap(stateId)
    assert groupingMap.stateId == stateId


# end interlude #


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
    sample.model_dump_json.return_value = "I like to eat, eat, eat"
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        with Config_override("samples.home", tempdir):
            filePath = f"{tempdir}/{sample.name}_{sample.unique_id}.json"
            localDataService.writeCalibrantSample(sample)
        assert os.path.exists(filePath)


def test_readCalibrantSample():  # noqa: ARG001
    localDataService = LocalDataService()
    sample = DAOFactory.sample_calibrant_sample
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        with Config_override("samples.home", tempdir):
            filePath = f"{tempdir}/{sample.name}_{sample.unique_id}.json"
            localDataService.writeCalibrantSample(sample)

            result = localDataService.readCalibrantSample(filePath)

    assert type(result) is CalibrantSample
    assert result.name == "NIST_640D"


def test_readCalibrantSample_does_not_exist():  # noqa: ARG001
    localDataService = LocalDataService()
    filePath = "GarbagePath"
    with pytest.raises(ValueError, match=f"The file '{filePath}' does not exist"):
        localDataService.readCalibrantSample(filePath)


@mock.patch("os.path.exists", return_value=True)
def test_readCifFilePath(mock1):  # noqa: ARG001
    localDataService = LocalDataService()

    result = localDataService.readCifFilePath("testid")
    assert result == "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/EntryWithCollCode52054_diamond.cif"


##### TESTS OF WORKSPACE WRITE METHODS #####


def test_writeWorkspace(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    workspaceName = "test_workspace"
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as tmpPath:
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".nxs")
        # Create a test workspace to write.
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=workspaceName,
        )
        cleanup_workspace_at_exit(workspaceName)
        assert mtd.doesExist(workspaceName)
        localDataService.writeWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()


def test_writeRaggedWorkspace(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix="/") as tmpPath:
        workspaceName = "test_ragged"
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".nxs")
        # Create a test ragged workspace to write.
        CreateSampleWorkspace(
            OutputWorkspace=workspaceName,
            Function="One Peak",
            NumBanks=1,
            NumMonitors=1,
            BankPixelWidth=5,
            NumEvents=500,
            Random=True,
            XUnit="DSP",
            XMin=0.5,
            XMax=8000,
            BinWidth=100,
        )
        cleanup_workspace_at_exit(workspaceName)
        cleanup_workspace_at_exit("test_out")
        assert mtd.doesExist(workspaceName)
        localDataService.writeWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()
        localDataService.readWorkspace(basePath, filename, "test_out")
        assert mtd.doesExist("test_out")


def test_writeGroupingWorkspace(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as tmpPath:
        workspaceName = "test_grouping"
        basePath = Path(tmpPath)
        filename = Path(workspaceName + ".h5")
        # Create a test grouping workspace to write.
        CreateGroupingWorkspace(
            OutputWorkspace=workspaceName,
            CustomGroupingString="1",
            InstrumentFilename=fakeInstrumentFilePath,
        )
        cleanup_workspace_at_exit(workspaceName)
        localDataService.writeGroupingWorkspace(basePath, filename, workspaceName)
        assert (basePath / filename).exists()


def test_writeDiffCalWorkspaces(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        tableWSName = "test_table"
        maskWSName = "test_mask"
        filename = Path(tableWSName + ".h5")
        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=instrumentDonor,
        )
        # Assign the required sample log values
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        addInstrumentLogs(instrumentDonor, **getInstrumentLogDescriptors(detectorState1))
        cleanup_workspace_at_exit(instrumentDonor)
        assert mtd.doesExist(instrumentDonor)

        # Create table and mask workspaces to write.
        createCompatibleMask(maskWSName, instrumentDonor)
        cleanup_workspace_at_exit(maskWSName)
        assert mtd.doesExist(maskWSName)
        createCompatibleDiffCalTable(tableWSName, instrumentDonor)
        cleanup_workspace_at_exit(tableWSName)
        assert mtd.doesExist(tableWSName)
        localDataService.writeDiffCalWorkspaces(
            basePath, filename, tableWorkspaceName=tableWSName, maskWorkspaceName=maskWSName
        )
        assert (basePath / filename).exists()


def test_writeDiffCalWorkspaces_mask_only(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        maskWSName = "test_mask"
        filename = Path(maskWSName + ".h5")
        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=instrumentDonor,
        )
        # Assign the required sample log values
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        addInstrumentLogs(instrumentDonor, **getInstrumentLogDescriptors(detectorState1))
        cleanup_workspace_at_exit(instrumentDonor)
        assert mtd.doesExist(instrumentDonor)

        # Create mask workspace to write.
        createCompatibleMask(maskWSName, instrumentDonor)
        cleanup_workspace_at_exit(maskWSName)
        assert mtd.doesExist(maskWSName)
        localDataService.writeDiffCalWorkspaces(basePath, filename, maskWorkspaceName=maskWSName)
        assert (basePath / filename).exists()


def test_writeDiffCalWorkspaces_bad_path(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with pytest.raises(  # noqa: PT012
        RuntimeError,
        match="specify filename including '.h5' extension",
    ):
        with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
            basePath = Path(basePath)
            tableWSName = "test_table"
            maskWSName = "test_mask"
            # do not add required: ".h5" suffix
            filename = Path(tableWSName)
            # Create an instrument workspace.
            instrumentDonor = "test_instrument_donor"
            LoadEmptyInstrument(
                Filename=fakeInstrumentFilePath,
                OutputWorkspace=instrumentDonor,
            )
            cleanup_workspace_at_exit(instrumentDonor)
            assert mtd.doesExist(instrumentDonor)
            # Assign the required sample log values
            detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
            addInstrumentLogs(instrumentDonor, **getInstrumentLogDescriptors(detectorState1))

            # Create table and mask workspaces to write.
            createCompatibleMask(maskWSName, instrumentDonor)
            cleanup_workspace_at_exit(maskWSName)
            assert mtd.doesExist(maskWSName)
            createCompatibleDiffCalTable(tableWSName, instrumentDonor)
            cleanup_workspace_at_exit(tableWSName)
            assert mtd.doesExist(tableWSName)
            localDataService.writeDiffCalWorkspaces(
                basePath, filename, tableWorkspaceName=tableWSName, maskWorkspaceName=maskWSName
            )
            assert (basePath / filename).exists()


def test_writePixelMask(cleanup_workspace_at_exit):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        maskWSName = "test_mask"
        filename = Path(maskWSName + ".h5")
        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        LoadEmptyInstrument(
            Filename=fakeInstrumentFilePath,
            OutputWorkspace=instrumentDonor,
        )
        # Assign the required sample log values
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        addInstrumentLogs(instrumentDonor, **getInstrumentLogDescriptors(detectorState1))
        cleanup_workspace_at_exit(instrumentDonor)
        assert mtd.doesExist(instrumentDonor)

        # Create mask workspace to write.
        createCompatibleMask(maskWSName, instrumentDonor)
        cleanup_workspace_at_exit(maskWSName)
        assert mtd.doesExist(maskWSName)
        localDataService.writePixelMask(basePath, filename, maskWorkspaceName=maskWSName)
        assert (basePath / filename).exists()


## TESTS OF REDUCTION PIXELMASK METHODS


class TestReductionPixelMasks:
    @pytest.fixture(autouse=True, scope="class")
    @classmethod
    def _setup_test_data(
        cls,
        create_sample_workspace,
        create_sample_pixel_mask,
    ):
        # Warning: the order of class `__init__` vs. autouse-fixture setup calls is ambiguous;
        #   for this reason, the `service` attribute, and anything that is initialized using it,
        #   is initialized _here_ in this fixture.
        cls.service = LocalDataService()

        cls.runNumber1 = "123456"
        cls.runNumber2 = "123457"
        cls.runNumber3 = "123458"
        cls.runNumber4 = "123459"
        cls.useLiteMode = True

        cls.timestamp1 = cls.service.getUniqueTimestamp()
        cls.timestamp2 = cls.service.getUniqueTimestamp()
        cls.timestamp3 = cls.service.getUniqueTimestamp()
        cls.timestamp4 = cls.service.getUniqueTimestamp()

        # Arbitrary, but distinct, `DetectorState`s used for realistic instrument initialization
        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

        # The corresponding stateId:
        cls.stateId1 = cls.service._stateIdFromDetectorState(cls.detectorState1).hex
        cls.stateId2 = cls.service._stateIdFromDetectorState(cls.detectorState2).hex

        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        instrumentFilePath = cls.instrumentLiteFilePath if cls.useLiteMode else cls.instrumentFilePath

        # create instrument workspaces for each state
        cls.sampleWS1 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS1, cls.detectorState1, instrumentFilePath)
        cls.sampleWS2 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS2, cls.detectorState2, instrumentFilePath)
        assert cls.service.stateIdFromWorkspace(cls.sampleWS1)[0] == cls.stateId1
        assert cls.service.stateIdFromWorkspace(cls.sampleWS2)[0] == cls.stateId2

        # random fraction used for mask initialization
        cls.randomFraction = 0.2

        # Create a pair of mask workspaces for each state
        cls.maskWS1 = wng.reductionUserPixelMask().numberTag(1).build()
        cls.maskWS2 = wng.reductionUserPixelMask().numberTag(2).build()
        cls.maskWS3 = wng.reductionPixelMask().runNumber(cls.runNumber3).timestamp(cls.timestamp3).build()
        cls.maskWS4 = wng.reductionPixelMask().runNumber(cls.runNumber4).timestamp(cls.timestamp4).build()
        create_sample_pixel_mask(cls.maskWS1, cls.detectorState1, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS2, cls.detectorState2, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS3, cls.detectorState1, instrumentFilePath, cls.randomFraction)
        create_sample_pixel_mask(cls.maskWS4, cls.detectorState2, instrumentFilePath, cls.randomFraction)
        assert cls.service.stateIdFromWorkspace(cls.maskWS1)[0] == cls.stateId1
        assert cls.service.stateIdFromWorkspace(cls.maskWS2)[0] == cls.stateId2
        assert cls.service.stateIdFromWorkspace(cls.maskWS3)[0] == cls.stateId1
        assert cls.service.stateIdFromWorkspace(cls.maskWS4)[0] == cls.stateId2
        yield

        # teardown...
        pass

    def _createReductionFileSystem(self):
        tss = (self.timestamp1, self.timestamp2)
        masks_ = {
            self.runNumber1: {tss[0]: self.maskWS1, tss[1]: self.maskWS3},
            self.runNumber2: {tss[0]: self.maskWS1, tss[1]: self.maskWS3},
            self.runNumber3: {tss[0]: self.maskWS2, tss[1]: self.maskWS4},
            self.runNumber4: {tss[0]: self.maskWS2, tss[1]: self.maskWS4},
        }
        for runNumber in (self.runNumber1, self.runNumber2, self.runNumber3, self.runNumber4):
            for ts in tss:
                # Warnings:
                # * this depends on `_constructReductionDataRoot` mock in `_setup_test_mocks`;
                # * this depends on `generateStateId` mock in `_setup_test_mocks`:
                #   so only a few run numbers will be set up to work.

                dataPath = self.service._constructReductionDataPath(runNumber, self.useLiteMode, ts)
                dataPath.mkdir(parents=True)
                maskFilePath = dataPath / (wng.reductionPixelMask().runNumber(runNumber).timestamp(ts).build() + ".h5")
                SaveDiffCal(MaskWorkspace=masks_[runNumber][ts], Filename=str(maskFilePath))
                assert maskFilePath.exists()

    @pytest.fixture(autouse=True)
    def _setup_test_mocks(
        self,
        monkeypatch,
        Config_override_fixture,
    ):
        monkeypatch.setattr(
            self.service,
            "generateStateId",
            lambda runNumber: {
                self.runNumber1: (self.stateId1, None),
                self.runNumber2: (self.stateId1, None),
                self.runNumber3: (self.stateId2, None),
                self.runNumber4: (self.stateId2, None),
            }[runNumber],
        )
        monkeypatch.setattr(
            self.service,
            "getIPTS",
            lambda runNumber: {
                self.runNumber1: "/SNS/SNAP/IPTS-1",
                self.runNumber2: "/SNS/SNAP/IPTS-1",
                self.runNumber3: "/SNS/SNAP/IPTS-2",
                self.runNumber4: "/SNS/SNAP/IPTS-2",
            }[runNumber],
        )

        stack = ExitStack()
        tmpPath = stack.enter_context(tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")))
        Config_override_fixture("instrument.reduction.home", tmpPath)
        self._createReductionFileSystem()
        # "instrument.<lite mode>.pixelResolution" is used by `isCompatibleMask`
        Config_override_fixture(
            "instrument." + ("lite" if self.useLiteMode else "native") + ".pixelResolution",
            # match non-monitor pixel count to the sample masks
            mtd[self.maskWS1].getInstrument().getNumberDetectors(True),
        )
        Config_override_fixture(
            "instrument." + ("lite" if not self.useLiteMode else "native") + ".pixelResolution",
            # arbitrary, incorrect pixel count:
            4 * mtd[self.maskWS1].getInstrument().getNumberDetectors(True),
        )
        yield

        # teardown...
        stack.close()

    def test_isCompatibleMask_state(self):
        assert self.service.isCompatibleMask(self.maskWS1, self.runNumber1, self.useLiteMode)
        assert not self.service.isCompatibleMask(self.maskWS1, self.runNumber3, self.useLiteMode)

    def test_isCompatibleMask_mode(self):
        assert self.service.isCompatibleMask(self.maskWS1, self.runNumber1, self.useLiteMode)
        assert not self.service.isCompatibleMask(self.maskWS1, self.runNumber1, not self.useLiteMode)

    def test_getCompatibleReductionMasks_resident(self):
        # Check that resident masks are compatible.

        # Compatible resident masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        for name in ["MaskWorkspace"]:
            assert name in masks
        for name in ["MaskWorkspace_2"]:
            assert name not in masks

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        for name in ["MaskWorkspace_2"]:
            assert name in masks
        for name in ["MaskWorkspace"]:
            assert name not in masks

    def test_getCompatibleReductionMasks_resident_as_WNG(self):
        # Check that resident masks are added as complete `WorkspaceName` (with builder).

        # Compatible resident masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        for name in masks:
            if "MaskWorkspace" not in name:
                # in this test: ignore non user-generated masks
                continue
            # Be careful here: `masks: List[WorkspaceName]` not `masks: List[str]`
            # => iterate over the `WorkspaceName`, not over the `str`.

            assert name in ["MaskWorkspace"]
            # somewhat complicated: `WorkspaceName` is an annotated type
            assert isinstance(name, typing.get_args(WorkspaceName)[0])
            assert name.tokens("workspaceType") == wngt.REDUCTION_USER_PIXEL_MASK

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        for name in masks:
            if "MaskWorkspace" not in name:
                continue
            assert name in ["MaskWorkspace_2"]
            # somewhat complicated: `WorkspaceName` is an annotated type
            assert isinstance(name, typing.get_args(WorkspaceName)[0])
            assert name.tokens("workspaceType") == wngt.REDUCTION_USER_PIXEL_MASK

    def test_getCompatibleReductionMasks_resident_pixel(self):
        # Check that any _resident_ pixel masks are compatible:
        #   this test checks against "reduction_pixelmask..." left over from
        #   any previous reductions.

        # Compatible, resident pixel mask => list should include this mask
        residentMask1 = wng.reductionPixelMask().runNumber(self.runNumber1).timestamp(self.timestamp1).build()
        CloneWorkspace(
            InputWorkspace=self.maskWS1,
            OutputWorkspace=residentMask1,
        )

        # Incompatible resident pixel mask => list should not include this mask
        # (This mask actually has an incompatible state.  We're avoiding running tests in native mode.)
        residentMask2 = wng.reductionPixelMask().runNumber(self.runNumber1).timestamp(self.timestamp2).build()
        CloneWorkspace(
            InputWorkspace=self.maskWS2,
            OutputWorkspace=residentMask2,
        )

        # To simplify this case, an incompatible resident "reduction_pixelmask" will be excluded,
        #   even of there is an on-disk version (with the same name) which would be compatible.
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        assert residentMask1 in masks
        assert residentMask2 not in masks

        DeleteWorkspaces(WorkspaceList=[residentMask1, residentMask2])

    def test_getCompatibleReductionMasks_nonresident(self):
        # Check that non-resident masks are compatible.

        # Compatible resident masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for name in masks:
            assert self.runNumber1 in name or self.runNumber2 in name
            assert self.runNumber3 not in name
            assert self.runNumber4 not in name

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for name in masks:
            assert self.runNumber3 in name or self.runNumber4 in name
            assert self.runNumber1 not in name
            assert self.runNumber2 not in name

    def test_getCompatibleReductionMasks_nonresident_as_WNG(self):
        # Check that non-resident masks are added as complete `WorkspaceName` (with builder).

        # Compatible resident masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for name in masks:
            assert self.runNumber1 in name or self.runNumber2 in name
            # somewhat complicated: `WorkspaceName` is an annotated type
            assert isinstance(name, typing.get_args(WorkspaceName)[0])
            assert name.tokens("workspaceType") == wngt.REDUCTION_PIXEL_MASK

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for name in masks:
            assert self.runNumber3 in name or self.runNumber4 in name
            # somewhat complicated: `WorkspaceName` is an annotated type
            assert isinstance(name, typing.get_args(WorkspaceName)[0])
            assert name.tokens("workspaceType") == wngt.REDUCTION_PIXEL_MASK

    def test_getCompatibleReductionMasks_nonresident_filenames(self):
        # Check that list of masks includes only valid file paths.

        # Compatible masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for mask in masks:
            runNumber, timestamp = mask.tokens("runNumber", "timestamp")
            filePath = self.service._constructReductionDataPath(runNumber, self.useLiteMode, timestamp) / (
                wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build() + ".h5"
            )
            assert filePath.exists()

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        # Ignore user-generated masks
        masks = [m for m in masks if "pixelmask" in m]
        for mask in masks:
            runNumber, timestamp = mask.tokens("runNumber", "timestamp")
            filePath = self.service._constructReductionDataPath(runNumber, self.useLiteMode, timestamp) / (
                wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build() + ".h5"
            )
            assert filePath.exists()

    def test_getCompatibleReductionMasks_no_duplicates(self):
        # Check that list of masks includes no duplicates.

        # Compatible resident masks: one check for each run number
        masks = self.service.getCompatibleReductionMasks(self.runNumber1, self.useLiteMode)
        duplicates: Set[WorkspaceName] = set()
        for name in masks:
            if name in duplicates:
                pytest.fail("masks list contains duplicate entries")
            duplicates.add(name)

        masks = self.service.getCompatibleReductionMasks(self.runNumber3, self.useLiteMode)
        duplicates = set()
        for name in masks:
            if name in duplicates:
                pytest.fail("masks list contains duplicate entries")
            duplicates.add(name)

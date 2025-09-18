import functools
import importlib
import inspect
import logging
import os
import re
import socket
import tempfile
import time
import typing
import unittest.mock as mock
from collections import namedtuple
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from random import randint, shuffle
from typing import List, Literal, Set, Tuple

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
    LoadNexusProcessed,
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
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.dao.request import (
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
    CreateNormalizationRecordRequest,
)
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state import DetectorState, InstrumentConfig, InstrumentState
from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.data.Indexer import IndexerType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.StateValidationException import StateValidationException
from snapred.backend.profiling.ProgressRecorder import _ProgressRecorder
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
from snapred.meta.redantic import parse_file_as, parse_obj_as, write_model_pretty

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


def mockGenerateStateId(runId: str) -> Tuple[ObjectSHA | None, DetectorState | None]:
    # Warning: in contrast to `LocalDataService.generateStateId`,
    #   this method returns the complete `ObjectSHA`, and not just the hex string

    detectorState = mockDetectorState(runId)
    if detectorState is None:
        return None, None
    return DetectorState.fromPVLogs(detectorState.toPVLogs(), DetectorState.LEGACY_SCHEMA).stateId, detectorState


def mockH5Dataset(value):
    dset = mock.MagicMock(spec=h5py.Dataset)
    dset.__getitem__.side_effect = lambda x: value  # noqa: ARG005
    return dset


def mockPVFile(detectorState: DetectorState, **kwargs) -> mock.Mock:
    # See also: `tests/unit/backend/data/util/test_PV_logs_util.py`.

    # Note: `PV_logs_util.mappingFromNeXusLogs` will open the 'entry/DASlogs' group,
    #   so this `dict` mocks the HDF5 group, not the PV-file itself.

    # For the HDF5-file, any "/value" suffix is _not_ part of the dataset's key!
    # Here we need the pre-type-normalized values (i.e. the time-series arrays).
    dict_ = {
        "start_time": np.array(["2023-06-14T14:06:40.429048667"]),
        "end_time": np.array(["2023-06-14T14:07:56.123123123"]),
        "BL3:Chop:Skf1:WavelengthUserReq": mockH5Dataset(np.array([detectorState["BL3:Chop:Skf1:WavelengthUserReq"]])),
        "det_arc1": np.array([detectorState["det_arc1"]]),
        "det_arc2": np.array([detectorState["det_arc2"]]),
        "BL3:Det:TH:BL:Frequency": np.array([detectorState["BL3:Det:TH:BL:Frequency"]]),
        "BL3:Mot:OpticsPos:Pos": np.array([detectorState["BL3:Mot:OpticsPos:Pos"]]),
        "det_lin1": np.array([detectorState["det_lin1"]]),
        "det_lin2": np.array([detectorState["det_lin2"]]),
    }
    dict_.update(kwargs)

    mock_ = mock.MagicMock(spec=h5py.Group)

    def _getitem(key: str):
        if key == "entry/DASlogs":
            return mock_
        return dict_[key]

    mock_.__getitem__.side_effect = _getitem
    return mock_


def mockNeXusLogsMapping(detectorState: DetectorState) -> mock.Mock:
    # See also: `tests/unit/backend/data/util/test_PV_logs_util.py`.

    dict_ = {
        "run_number": "123456",
        "start_time": datetime.fromisoformat("2023-06-14T14:06:40.429048"),
        "end_time": datetime.fromisoformat("2023-06-14T14:07:56.123123"),
        "proton_charge": 1000.0,
        "BL3:Chop:Skf1:WavelengthUserReq": [detectorState.wav],
        "det_arc1": [detectorState["det_arc1"]],
        "det_arc2": [detectorState["det_arc2"]],
        "BL3:Det:TH:BL:Frequency": [detectorState["BL3:Det:TH:BL:Frequency"]],
        "BL3:Mot:OpticsPos:Pos": [detectorState["BL3:Mot:OpticsPos:Pos"]],
        "det_lin1": [detectorState["det_lin1"]],
        "det_lin2": [detectorState["det_lin2"]],
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
        with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing"):
            getattr(localDataService, f"read{workflow}Index")(True, "stateId")


def do_test_workflow_indexer(workflow):
    # verify the correct indexer is being returned
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, f"{workflow.lower()}Indexer") as mockIndexer:
        for useLiteMode in [True, False]:
            getattr(localDataService, f"{workflow.lower()}Indexer")(useLiteMode, "stateId")
            mockIndexer.assert_called_once_with(useLiteMode, "stateId")
            mockIndexer.reset_mock()


def do_test_read_index(workflow):
    # verify that calls to read index call out to the indexer
    mockIndex = ["nope"]
    mockIndexer = mock.Mock(getIndex=mock.Mock(return_value=mockIndex))
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, f"{workflow.lower()}Indexer", mock.Mock(return_value=mockIndexer)):
        for useLiteMode in [True, False]:
            ans = getattr(localDataService, f"read{workflow}Index")(useLiteMode, "stateId")
            assert ans == mockIndex


def do_test_read_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=("stateId", None))
    with mock.patch.object(localDataService, f"{workflow.lower()}Indexer", mock.Mock(return_value=mockIndexer)):
        for useLiteMode in [True, False]:
            version = randint(1, 20)
            res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode, "stateId", version)
            assert res == mockIndexer.readRecord.return_value
            mockIndexer.readRecord.assert_called_with(version)


def do_test_read_record_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the functionality in the indexer
    # if no version is given, it should get latest applicable version
    latestVersion = randint(20, 120)
    mockIndexer = mock.Mock(latestApplicableVersion=mock.Mock(return_value=latestVersion))
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=("stateId", None))
    with mock.patch.object(localDataService, f"{workflow.lower()}Indexer", mock.Mock(return_value=mockIndexer)):
        for useLiteMode in [True, False]:
            res = getattr(localDataService, f"read{workflow}Record")("xyz", useLiteMode, "stateId")  # NOTE no version
            mockIndexer.latestApplicableVersion.assert_called_with("xyz")
            mockIndexer.readRecord.assert_called_with(latestVersion)
            assert res == mockIndexer.readRecord.return_value


def do_test_write_record_with_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    # ensure it is calling the methods inside the indexer service
    mockIndexer = mock.Mock()
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=("stateId", None))
    with mock.patch.object(localDataService, f"{workflow.lower()}Indexer", mock.Mock(return_value=mockIndexer)):
        for useLiteMode in [True, False]:
            indexEntry = IndexEntry(
                runNumber="123",
                useLiteMode=useLiteMode,
                appliesTo=">=1",
                comments="test comment",
                author="test author",
                version=randint(1, 120),
            )
            record = globals()[f"{workflow}Record"].model_construct(
                runNumber="xyz",
                useLiteMode=useLiteMode,
                workspaces={},
                version=indexEntry.version,
                calculationParameters=mock.Mock(),
                indexEntry=indexEntry,
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
                indexerMethod = getattr(localDataService, f"{workflow.lower()}Indexer")
                indexer = indexerMethod(useLiteMode, tmpRoot.stateId)
                indexer.readIndex = mock.Mock()
                tmpRoot.saveObjectAt(expectedState, indexer.parametersPath(version))
                assert indexer.parametersPath(version).exists()
                actualState = getattr(localDataService, f"read{workflow}State")(
                    "xyz", useLiteMode, tmpRoot.stateId, version
                )
            assert actualState == expectedState


def do_test_read_state_no_version(workflow: Literal["Calibration", "Normalization", "Reduction"]):
    currentVersion = randint(20, 120)
    localDataService = LocalDataService()

    for useLiteMode in [True, False]:
        with state_root_redirect(localDataService) as tmpRoot:
            localDataService.generateStateId = mock.Mock(return_value=(tmpRoot.stateId, None))
            expectedParameters = getattr(DAOFactory, f"{workflow.lower()}Parameters")()
            indexerMethod = getattr(localDataService, f"{workflow.lower()}Indexer")
            indexer = indexerMethod(useLiteMode, tmpRoot.stateId)
            indexer.readIndex = mock.Mock()
            tmpRoot.saveObjectAt(expectedParameters, indexer.parametersPath(currentVersion))
            indexer.index = {
                currentVersion: mock.MagicMock(appliesTo="123", version=currentVersion)
            }  # NOTE manually update indexer
            indexer = mock.MagicMock(wraps=indexer)
            indexer.dirVersions = [currentVersion]  # NOTE manually update indexer
            with mock.patch.object(localDataService, f"{workflow.lower()}Indexer", mock.Mock(return_value=indexer)):
                actualParameters = getattr(localDataService, f"read{workflow}State")(
                    "123", useLiteMode, tmpRoot.stateId, VersionState.LATEST
                )  # NOTE no version
        assert actualParameters == expectedParameters


def test_write_instrument_parameters():
    localDataService = LocalDataService()
    localDataService.instrumentParameterIndexer = mock.Mock()
    mockIndexer = mock.Mock()
    mockEntry = mock.Mock()
    mockIndexer.createIndexEntry = mock.Mock(return_value=mockEntry)
    localDataService.instrumentParameterIndexer.return_value = mockIndexer
    mockInstrumentParameters = mock.Mock(spec=InstrumentConfig)
    localDataService.writeInstrumentParameters(mockInstrumentParameters, ">1,<2", "test author")
    mockIndexer.writeIndexedObject.assert_called_with(mockInstrumentParameters)


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
    instrumentParameters = parse_file_as(InstrumentConfig, Resource.getPath("inputs/SNAPInstPrm.json"))
    return instrumentParameters


def test_readInstrumentParameters():
    localDataService = LocalDataService()
    mockIndexer = mock.Mock()
    mockIndexer.readIndexedObject = mock.Mock(return_value=_readInstrumentParameters())
    localDataService.instrumentParameterIndexer = mock.Mock(return_value=mockIndexer)
    actual = localDataService.readInstrumentParameters(123)
    assert actual is not None
    assert actual.version == 0
    assert actual.name == "SNAP"


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
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer(True, "stateId")
        indexer.readIndex = mock.Mock()
        parameters = DAOFactory.calibrationParameters("57514", True, indexer.defaultVersion())
        groupingMap = DAOFactory.groupingMap_SNAP(parameters.instrumentState.id)
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(indexer.defaultVersion()))

        indexer.index = {
            VersionState.DEFAULT: mock.MagicMock(appliesTo="57514", version=indexer.defaultVersion())
        }  # NOTE manually update the Indexer
        with mock.patch.object(localDataService, "calibrationIndexer", mock.Mock(return_value=indexer)):
            actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == parameters.instrumentState.id.hex


def test_readStateConfig_previous():
    # readStateConfig will load the previous version's parameters file
    version = randint(2, 10)
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    groupingMap = DAOFactory.groupingMap_SNAP(parameters.instrumentState.id)
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=(groupingMap.stateId, None))
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer(True, "stateId")
        indexer.readIndex = mock.Mock()
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        with mock.patch.object(localDataService, "calibrationIndexer", mock.Mock(return_value=indexer)):
            actual = localDataService.readStateConfig("57514", True)
    assert actual is not None
    assert actual.stateId == parameters.instrumentState.id.hex


def test_readStateConfig_attaches_grouping_map():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    groupingMap = DAOFactory.groupingMap_SNAP(parameters.instrumentState.id)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer(True, "stateId")
        indexer.readIndex = mock.Mock()
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        with mock.patch.object(localDataService, "calibrationIndexer", mock.Mock(return_value=indexer)):
            actual = localDataService.readStateConfig("57514", True)
    expectedMap = DAOFactory.groupingMap_SNAP(parameters.instrumentState.id)
    assert actual.groupingMap == expectedMap


def test_readStateConfig_invalid_grouping_map():
    # Test that the attached grouping-schema map's 'stateId' is checked.
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    groupingMap = DAOFactory.groupingMap_SNAP(DAOFactory.nonsense_state_id)
    parameters = DAOFactory.calibrationParameters("57514", True, version)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService) as tmpRoot:
        indexer = localDataService.calibrationIndexer(True, "stateId")
        indexer.readIndex = mock.Mock()
        tmpRoot.saveObjectAt(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))
        tmpRoot.saveObjectAt(parameters, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        # 'GroupingMap.defaultStateId' is _not_ a valid grouping-map 'stateId' for an existing `StateConfig`.
        with mock.patch.object(localDataService, "calibrationIndexer", mock.Mock(return_value=indexer)):
            with pytest.raises(  # noqa: PT012
                RuntimeError,
                match="the state configuration's grouping map must have the same 'stateId' as the configuration",
            ):
                localDataService.readStateConfig("57514", True)


def test_readStateConfig_calls_prepareStateRoot():
    # test that `readStateConfig` reads the `GroupingMap` from its separate JSON file.
    version = randint(2, 10)
    expected = DAOFactory.calibrationParameters("57514", True, version)
    groupingMap = DAOFactory.groupingMap_SNAP(expected.instrumentState.id)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=expected.instrumentState.id.hex) as tmpRoot:
        indexer = localDataService.calibrationIndexer(True, "stateId")
        indexer.readIndex = mock.Mock()
        tmpRoot.saveObjectAt(expected, indexer.parametersPath(version))
        indexer.index = {
            version: mock.MagicMock(appliesTo="57514", version=version)
        }  # NOTE manually update the Indexer
        with mock.patch.object(localDataService, "calibrationIndexer", mock.Mock(return_value=indexer)):
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


@mock.patch(ThisService + "RunMetadata")
def test_readRunMetadata(mockRunMetadata):
    runNumber = "12345"
    mockMetadata = mock.Mock(spec=RunMetadata, runNumber=runNumber)
    mockRunMetadata.fromNeXusLogs.return_value = mockMetadata
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=mock.sentinel.stateIdSchema)
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(spec=h5py.File, return_value=mock.sentinel.h5)
    instance.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)

    actual = instance.readRunMetadata(runNumber)
    assert actual == mockMetadata
    mockRunMetadata.fromNeXusLogs.assert_called_once_with(mock.sentinel.h5, mock.sentinel.stateIdSchema)


@mock.patch(ThisService + "RunMetadata")
def test_readRunMetadata_runNumber_mismatch(mockRunMetadata):
    runNumber1 = "12345"
    runNumber2 = "67890"
    mockMetadata = mock.Mock(spec=RunMetadata, runNumber=runNumber2)
    mockRunMetadata.fromNeXusLogs.return_value = mockMetadata
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=mock.sentinel.stateIdSchema)
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(spec=h5py.File, return_value=mock.sentinel.h5)
    instance.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)

    with pytest.raises(RuntimeError, match=f"Expected run-number '{runNumber1}' from the filename does not match.*"):
        actual = instance.readRunMetadata(runNumber1)  # noqa: F841


@mock.patch(ThisService + "RunMetadata")
def test_readRunMetadata_live_data_fallback(mockRunMetadata):
    runNumber = "12345"
    mockMetadata = mock.Mock(spec=RunMetadata, runNumber=runNumber)
    mockRunMetadata.fromRun.return_value = mockMetadata
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("No PVFile exists"))
    instance.readInstrumentConfig = mock.Mock()
    instance.hasLiveDataConnection = mock.Mock(return_value=True)
    instance.readLiveMetadata = mock.Mock(return_value=mockMetadata)

    actual = instance.readRunMetadata(runNumber)
    assert actual == mockMetadata
    instance.readInstrumentConfig.assert_not_called()
    instance.hasLiveDataConnection.assert_called_once()


@mock.patch(ThisService + "RunMetadata")
def test_readRunMetadata_live_data_fallback_fails(mockRunMetadata):
    runNumber1 = "12345"
    runNumber2 = "67890"
    mockMetadata = mock.Mock(spec=RunMetadata, runNumber=runNumber2)
    mockRunMetadata.fromRun.return_value = mockMetadata
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("No PVFile exists"))
    instance.hasLiveDataConnection = mock.Mock(return_value=True)
    instance.readLiveMetadata = mock.Mock(return_value=mockMetadata)

    with pytest.raises(RuntimeError, match=".*it isn't the live run.*"):
        actual = instance.readRunMetadata(runNumber1)  # noqa: F841


@mock.patch(ThisService + "RunMetadata")
def test_readRunMetadata_live_data_fallback_no_active_run(mockRunMetadata):
    runNumber = "12345"
    mockMetadata = mock.Mock(
        spec=RunMetadata,
        runNumber=0,
    )
    mockMetadata.hasActiveRun.return_value = False
    mockRunMetadata.fromRun.return_value = mockMetadata
    instance = LocalDataService()
    instance._readPVFile = mock.Mock(side_effect=FileNotFoundError("No PVFile exists"))
    instance.hasLiveDataConnection = mock.Mock(return_value=True)
    instance.readLiveMetadata = mock.Mock(return_value=mockMetadata)

    with pytest.raises(RuntimeError, match=".*no live run is active.*"):
        actual = instance.readRunMetadata(runNumber)  # noqa: F841


@mock.patch("socket.gethostbyaddr")
def test_hasLiveDataConnection(mockGetHostByAddr):
    with Config_override("liveData.enabled", True):
        liveDataHostname = "bl3-daq1.sns.gov"
        liveDataIPV4 = "10.111.6.150"
        mockGetHostByAddr.return_value = (liveDataHostname, [], [liveDataIPV4])
        instance = LocalDataService()
        assert instance.hasLiveDataConnection()
        mockGetHostByAddr.assert_called_once_with(liveDataHostname)


@mock.patch("socket.gethostbyaddr")
def test_hasLiveDataConnection_no_connection(mockGetHostByAddr):
    with Config_override("liveData.enabled", True):
        mockGetHostByAddr.side_effect = RuntimeError("no live connection")
        instance = LocalDataService()
        assert not instance.hasLiveDataConnection()


@mock.patch("socket.gethostbyaddr")
def test_hasLiveDataConnection_config_disabled(mockGetHostByAddr):
    with Config_override("liveData.enabled", False):
        assert not Config["liveData.enabled"]
        instance = LocalDataService()
        assert not instance.hasLiveDataConnection()
        mockGetHostByAddr.assert_not_called()


@mock.patch(ThisService + "RunMetadata")
def test__liveMetadataFromRun(mockRunMetadata):
    mockMetadata = mock.Mock(spec=RunMetadata)
    mockRunMetadata.fromRun.return_value = mockMetadata
    mockRun = mock.Mock(
        spec=Run,
        hasProperty=mock.Mock(return_value=True),
        getProperty=mock.Mock(return_value=mock.Mock(value=mock.sentinel.runNumber)),
    )
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=mock.sentinel.stateIdSchema)

    instance = LocalDataService()
    instance.readInstrumentParameters = mock.Mock(return_value=mockInstrumentConfig)

    actual = instance._liveMetadataFromRun(mockRun)
    assert actual == mockMetadata
    mockRunMetadata.fromRun.assert_called_once_with(mockRun, mock.sentinel.stateIdSchema, liveData=True)


@mock.patch(ThisService + "RunMetadata")
def test__liveMetadataFromRun_exception_routing(mockRunMetadata):
    # Verify that:
    #   -- the appropriate `RunMetadata` extraction exceptions are routed to `RuntimeError`;
    #   -- any other exception type is passed through.

    mockMetadata = mock.Mock(spec=RunMetadata)
    for fromRunException in (
        KeyError("some key"),
        RuntimeError("some mantid error"),
        ValueError("some other validation error"),
    ):
        mockRunMetadata.fromRun.side_effect = fromRunException
        mockRun = mock.Mock(
            spec=Run,
            hasProperty=mock.Mock(return_value=True),
            getProperty=mock.Mock(return_value=mock.Mock(value=mock.sentinel.runNumber)),
        )
        mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=mock.sentinel.stateIdSchema)

        instance = LocalDataService()
        instance.readInstrumentParameters = mock.Mock(return_value=mockInstrumentConfig)

        with pytest.raises(RuntimeError, match="Unable to extract RunMetadata from Run"):
            actual = instance._liveMetadataFromRun(mockRun)  # noqa: F841
        mockMetadata.reset_mock()
        mockRunMetadata.reset_mock()

    class SomeOtherException(Exception):
        def __init__(self, msg: str):
            super().__init__()
            self._msg = msg

        def __str__(self):
            return self._msg

    mockRunMetadata.fromRun.side_effect = SomeOtherException("any other exception")
    with pytest.raises(SomeOtherException, match="any other exception"):
        actual = instance._liveMetadataFromRun(mockRun)  # noqa: F841


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
def test_calibrationFileExists(pathExists):
    pathExists.return_value = True
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, "getIPTS"):
        stateId = ENDURING_STATE_ID
        with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
            tmpRoot.path().mkdir()
            runNumber = "654321"
            assert localDataService.checkCalibrationFileExists(runNumber)


def test_calibrationFileExists_stupid_number():
    localDataService = LocalDataService()

    with mock.patch.object(localDataService, "getIPTS") as mockGetIPTS:
        # try with a non-number
        runNumber = "fruitcake"
        assert not localDataService.checkCalibrationFileExists(runNumber)
        mockGetIPTS.assert_not_called()
        mockGetIPTS.reset_mock()

        # try with a too-small number
        runNumber = "7"
        assert not localDataService.checkCalibrationFileExists(runNumber)
        mockGetIPTS.assert_not_called()


def test_calibrationFileExists_not():
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, "getIPTS"):
        stateId = ENDURING_STATE_ID
        with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
            nonExistentPath = tmpRoot.path() / "1755"
            assert not nonExistentPath.exists()
            runNumber = "654321"
            assert not localDataService.checkCalibrationFileExists(runNumber)


def test_calibrationFileExists_no_PV_file():
    instance = LocalDataService()
    with mock.patch.object(instance, "getIPTS"):
        stateId = ENDURING_STATE_ID  # noqa: F841
        runNumber = "654321"
        with mock.patch.object(instance, "generateStateId") as mockGenerateStateId:
            # WARNING: the actual exception would normally be re-routed to `StateValidationException`.
            mockGenerateStateId.side_effect = FileNotFoundError("No PV-file")
            assert not instance.checkCalibrationFileExists(runNumber)


def test_calibrationFileExists_no_PV_file_exception_routing():
    instance = LocalDataService()
    with mock.patch.object(instance, "getIPTS"):
        runNumber = "654321"
        with pytest.raises(StateValidationException):
            instance.checkCalibrationFileExists(runNumber)


@mock.patch("pathlib.Path.exists")
def test_getIPTS(mockPathExists):
    mockPathExists.return_value = True
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, "mantidSnapper") as mockSnapper:
        mockSnapper.CheckIPTS = mock.Mock(return_value="nowhere/")
        runNumber = "123456"
        res = localDataService.getIPTS(runNumber)
        assert res == Path(mockSnapper.CheckIPTS.return_value)
        mockSnapper.CheckIPTS.assert_called_with(
            "get IPTS directory", RunNumber=runNumber, Instrument=Config["instrument.name"], ClearCache=True
        )
        mockSnapper.CheckIPTS.reset_mock()

        res = localDataService.getIPTS(runNumber, "CRACKLE")
        assert res == Path(mockSnapper.CheckIPTS.return_value)
        mockSnapper.CheckIPTS.assert_called_with(
            "get IPTS directory", RunNumber=runNumber, Instrument="CRACKLE", ClearCache=True
        )


def test__getIPTS_no_IPTS():
    # Verify that the "no IPTS directory" case is cached internally as `None`:
    localDataService = LocalDataService()
    with mock.patch.object(localDataService, "mantidSnapper") as mockSnapper:
        mockSnapper.CheckIPTS = mock.Mock(return_value="")
        runNumber = "123456"
        result = localDataService.getIPTS(runNumber)
        assert result is None


def test_getIPTS_error():
    # Verify that any exception from `CheckIPTS` is passed through.

    class _OtherError(Exception):
        def __init__(self, msg):
            super().__init__()
            self._msg = msg

        def __str__(self):
            return self._msg

    localDataService = LocalDataService()
    with mock.patch.object(localDataService, "mantidSnapper") as mockSnapper:
        mockSnapper.CheckIPTS = mock.Mock(side_effect=_OtherError("No one has ever seen this error"))
        runNumber = "123456"
        with pytest.raises(_OtherError, match="No one has ever seen this error"):
            result = localDataService.getIPTS(runNumber)  # noqa: F841


# NOTE this test calls `CheckIPTS` (via `getIPTS`) with no mocks
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

    # Direct `CheckIPTS` to look in the exact folder where it should look
    #   -- it is very stupid, so if you don't tell it exactly then it won't look there.
    with amend_config(data_dir=str(correctIPTS / "nexus")):
        res = localDataService.getIPTS(*key)
        assert res == correctIPTS
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)

        # call again and make sure the cached value is being returned
        res = localDataService.getIPTS(*key)
        assert res == correctIPTS
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=1, misses=1, maxsize=128, currsize=1)

    # now try it again, but with another IPTS directory
    with amend_config(data_dir=str(incorrectIPTS / "nexus")):
        # previous correct value should still be the cached value
        res = localDataService.getIPTS(*key)
        assert res == correctIPTS
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=2, misses=1, maxsize=128, currsize=1)

        # clear the cache,  make sure the new value is being returned
        localDataService.getIPTS.cache_clear()
        res = localDataService.getIPTS(*key)
        assert res == incorrectIPTS
        assert localDataService.getIPTS.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)


def test_createNeutronFilePath():
    instance = LocalDataService()
    with mock.patch.object(instance, "getIPTS") as mockGetIPTS:
        mockGetIPTS.return_value = Path("IPTS-TEST")
        runNumbers = {"lite": "12345", "native": "67890"}

        for mode in ["lite", "native"]:
            runNumber = runNumbers[mode]  # REMINDER: `LocalDataService.getIPTS` uses `@lru_cache`.
            expected = mockGetIPTS.return_value / (
                Config[f"nexus.{mode}.prefix"] + runNumber + Config[f"nexus.{mode}.extension"]
            )

            actual = instance.createNeutronFilePath(runNumber, mode == "lite")
            mockGetIPTS.assert_called_once_with(runNumber)
            assert actual == expected
            mockGetIPTS.reset_mock()


def test_stateExists():
    instance = LocalDataService()
    with (
        mock.patch.object(instance, "generateStateId") as mockGenerateStateId,
        mock.patch.object(instance, "constructCalibrationStateRoot") as mockConstructCalibrationStateRoot,
    ):
        mockGenerateStateId.return_value = (mock.sentinel.SHA, mock.sentinel.ignored)
        mockConstructCalibrationStateRoot.return_value = mock.Mock(
            spec=Path, exists=mock.Mock(return_value=mock.sentinel.exists)
        )
        runNumber = "12345"
        actual = instance.stateExists(runNumber)
        assert actual == mock.sentinel.exists
        mockGenerateStateId.assert_called_once_with(runNumber)
        mockConstructCalibrationStateRoot.assert_called_once_with(mock.sentinel.SHA)
        mockConstructCalibrationStateRoot.return_value.exists.assert_called_once()


def test_workspaceIsInstance(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    localDataService = LocalDataService()

    # Create a sample workspace.
    testWS0 = "test_ws"
    runNumber = "12345"
    runTitle = "ws for 12345"
    detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
    create_per_test_sample_workspace(testWS0, detectorState1, fakeInstrumentFilePath, runNumber, runTitle)
    assert mtd.doesExist(testWS0)
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
        indexer = localDataService.calibrationIndexer(True, "stateId")
        record = DAOFactory.calibrationRecord("57514", True, indexer.defaultVersion())
        indexer.writeRecord(record)
        indexer.writeParameters(DAOFactory.calibrationParameters("57514", True, indexer.defaultVersion()))
        indexer.index = {indexer.defaultVersion(): mock.MagicMock(appliesTo="57514", version=indexer.defaultVersion())}

        # move the grouping map into correct folder
        groupingMap = DAOFactory.groupingMap_SNAP(record.calculationParameters.instrumentState.id)
        write_model_pretty(groupingMap, localDataService._groupingMapPath(tmpRoot.stateId))

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
    localDataService._readRunConfig = mock.Mock(return_value=mock.sentinel.RunConfig)
    actual = localDataService.readRunConfig(mock.Mock())
    assert actual == mock.sentinel.RunConfig


def test__readRunConfig():
    # Test of private `_readRunConfig` method
    localDataService = LocalDataService()
    runNumber = "57514"
    localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-123"))
    localDataService.readInstrumentConfig = mock.Mock(return_value=getMockInstrumentConfig())
    actual = localDataService._readRunConfig(runNumber)
    assert actual.runNumber == runNumber


def test__readRunConfig_no_IPTS():
    # Test of private `_readRunConfig` method
    localDataService = LocalDataService()
    runNumber = "57514"
    localDataService.getIPTS = mock.Mock(side_effect=RuntimeError("Cannot find IPTS directory"))
    with pytest.raises(RuntimeError, match="Cannot find IPTS directory"):
        actual = localDataService._readRunConfig(runNumber)  # noqa: F841


def test_constructPVFilePath():
    # ensure the file path points to inside the IPTS folder
    runNumber = "12345"
    localDataService = LocalDataService()
    # mock the IPTS to point to somewhere then construct the path
    mockIPTS: Path = Path(Resource.getPath("inputs/testInstrument/IPTS-456"))
    with mock.patch.object(localDataService, "getIPTS", mock.Mock(return_value=mockIPTS)):
        path = localDataService._constructPVFilePath(runNumber)
        # the path should be /path/to/testInstrument/IPTS-456/nexus/SNAP_<runNumber>.nxs.h5
        assert mockIPTS == path.parents[1]
        localDataService.getIPTS.assert_called_once_with(runNumber)


@mock.patch("h5py.File", return_value="not None")
def test_readPVFile(h5pyMock):  # noqa: ARG001
    localDataService = LocalDataService()
    localDataService._constructPVFilePath = mock.Mock(return_value=mock.Mock(spec=Path))
    actual = localDataService._readPVFile(mock.Mock())
    assert actual is not None


@mock.patch("h5py.File", return_value="not None")
def test_readPVFile_does_not_exist(h5pyMock):  # noqa: ARG001
    runNumber = "12345"
    localDataService = LocalDataService()

    PVFilePath = "/the/PVFile.nxs.h5"
    mockPath = mock.MagicMock(spec=Path)
    mockPath.__str__.return_value = PVFilePath
    mockPath.exists.return_value = False
    localDataService._constructPVFilePath = mock.Mock(return_value=mockPath)
    with pytest.raises(FileNotFoundError, match=f"No PVFile exists for run: '{runNumber}'"):
        localDataService._readPVFile(runNumber)


def test_readPVFile_no_IPTS():
    # Test that `_readPVFile` raises `FileNotFound` when the PVFile path is None.
    runNumber = "12345"
    localDataService = LocalDataService()
    localDataService._constructPVFilePath = mock.Mock(return_value=None)
    with pytest.raises(FileNotFoundError, match=f"No PVFile exists for run: '{runNumber}'"):
        localDataService._readPVFile(runNumber)


def test_readPVFile_exception_passthrough():
    # Test that `_readPVFile` passes unrelated exceptions without any modification.
    runNumber = "12345"
    localDataService = LocalDataService()
    localDataService._constructPVFilePath = mock.Mock(side_effect=RuntimeError("lah dee dah"))
    with pytest.raises(RuntimeError, match="lah dee dah"):
        localDataService._readPVFile(runNumber)


@mock.patch(ThisService + "RunMetadata")
def test_generateStateId(mockRunMetadata):
    runNumber = "12345"
    stateId, detectorState = mockGenerateStateId(runNumber)
    mockRunMetadata.fromNeXusLogs.return_value = RunMetadata.model_construct(
        runNumber=runNumber, detectorState=detectorState, stateId=stateId
    )

    service = LocalDataService()
    service._readPVFile = mock.Mock(return_value=mockPVFile(detectorState))
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA)
    service.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)

    actual = service.generateStateId("12345")
    assert actual == (stateId.hex, detectorState)


@mock.patch(ThisService + "RunMetadata")
def test_generateStateId_cache(mockRunMetadata):
    service = LocalDataService()
    service.generateStateId.cache_clear()
    assert service.generateStateId.cache_info() == functools._CacheInfo(hits=0, misses=0, maxsize=128, currsize=0)

    service._readPVFile = mock.Mock(
        side_effect=lambda runNumber: mockPVFile(
            mockDetectorState(runNumber),
            run_number=np.array(
                [
                    runNumber.encode("utf8"),
                ]
            ),
        )
    )
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA)
    service.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)

    runNumber1 = "12345"
    stateSHA1, detectorState1 = mockGenerateStateId(runNumber1)
    metadata1 = RunMetadata.model_construct(runNumber=runNumber1, detectorState=detectorState1, stateId=stateSHA1)

    runNumber2 = "67890"
    stateSHA2, detectorState2 = mockGenerateStateId(runNumber2)
    metadata2 = RunMetadata.model_construct(runNumber=runNumber2, detectorState=detectorState2, stateId=stateSHA2)

    def selectMetadata(logs, _stateIdSchema) -> RunMetadata:
        runNumber = str(logs["run_number"][0], encoding="utf8")
        match runNumber:
            case "12345":
                return metadata1
            case "67890":
                return metadata2
            case _:
                raise RuntimeError(f"Test setup error: no `RunMetdata` has been initialized for run '{runNumber}'.")

    mockRunMetadata.fromNeXusLogs.side_effect = selectMetadata

    actual = service.generateStateId(runNumber1)
    assert actual == (stateSHA1.hex, detectorState1)
    assert service.generateStateId.cache_info() == functools._CacheInfo(hits=0, misses=1, maxsize=128, currsize=1)

    # check cached value
    actual = service.generateStateId(runNumber1)
    assert actual == (stateSHA1.hex, detectorState1)
    assert service.generateStateId.cache_info() == functools._CacheInfo(hits=1, misses=1, maxsize=128, currsize=1)

    # check a different value
    actual = service.generateStateId(runNumber2)
    assert actual == (stateSHA2.hex, detectorState2)
    assert service.generateStateId.cache_info() == functools._CacheInfo(hits=1, misses=2, maxsize=128, currsize=2)
    # ... and its cached value
    actual = service.generateStateId(runNumber2)
    assert actual == (stateSHA2.hex, detectorState2)
    assert service.generateStateId.cache_info() == functools._CacheInfo(hits=2, misses=2, maxsize=128, currsize=2)


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
    fakeIPTS = Path("gumdrop")
    fakeState = "joobiewoobie"
    localDataService = LocalDataService()
    localDataService.getIPTS = mock.Mock(return_value=fakeIPTS)
    localDataService.generateStateId = mock.Mock(return_value=(fakeState, "gibberish"))
    runNumber = "xyz"
    ans = localDataService._constructReductionStateRoot(runNumber)
    assert isinstance(ans, Path)
    assert ans.parts[-1] == fakeState
    assert str(fakeIPTS) in ans.parts


def test_constructReductionStateRoot_no_IPTS():
    localDataService = LocalDataService()
    runNumber = "12345"
    localDataService.getIPTS = mock.Mock(return_value=None)
    localDataService.generateStateId = mock.Mock(return_value=(mock.sentinel.SHA, mock.sentinel.ignored))
    with pytest.raises(RuntimeError, match=".*Cannot find IPTS directory.*"):
        actual = localDataService._constructReductionStateRoot(runNumber)  # noqa: F841


def test_constructReductionDataRoot():
    fakeIPTS = Path("gumdrop")
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


def test_calibrationIndexer():
    do_test_workflow_indexer("Calibration")


def test_calibrationIndexer_alternate():
    localDataService = LocalDataService()
    indexer = localDataService.calibrationIndexer(True, "altStateTest")
    assert "altStateTest" in str(indexer.rootDirectory)


def test_normalizationIndexer():
    do_test_workflow_indexer("Normalization")


def test_readCalibrationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Calibration")


def test_readNormalizationIndex():
    # verify that calls to read index call to the indexer
    do_test_read_index("Normalization")


def test_obtainNormalizationLock():
    # verify that the lock is obtained and released correctly
    localDataService = LocalDataService()
    normalizationIndexer = localDataService.normalizationIndexer(True, "stateId")

    lock = localDataService.obtainNormalizationLock(True, "stateId")
    lockfileContents = lock.lockFilePath.read_text()

    assert lock is not None
    assert str(normalizationIndexer.rootDirectory) in lockfileContents
    lock.release()


def test_obtainCalibrationLock():
    # verify that the lock is obtained and released correctly
    localDataService = LocalDataService()
    calibrationIndexer = localDataService.calibrationIndexer(True, "stateId")

    lock = localDataService.obtainCalibrationLock(True, "stateId")
    lockfileContents = lock.lockFilePath.read_text()

    assert lock is not None
    assert str(calibrationIndexer.rootDirectory) in lockfileContents
    lock.release()


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
        actualEntries = localDataService.readCalibrationIndex(entry.useLiteMode, "stateId")
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
        actualEntries = localDataService.readNormalizationIndex(entry.useLiteMode, "stateId")
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
        localDataService.calibrationIndexer(request.useLiteMode, "stateId")
        ans = localDataService.createCalibrationIndexEntry(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START()


def test_createCalibrationRecord():
    record = DAOFactory.calibrationRecord("57514", True, 1)
    recordDump = record.model_dump()
    del recordDump["snapredVersion"]
    del recordDump["snapwrapVersion"]
    request = CreateCalibrationRecordRequest(**recordDump)
    localDataService = LocalDataService()
    with state_root_redirect(localDataService):
        ans = localDataService.createCalibrationRecord(request)
        assert isinstance(ans, CalibrationRecord)
        assert ans.runNumber == request.runNumber
        assert ans.useLiteMode == request.useLiteMode
        assert ans.version == request.version

        request.version = VersionState.NEXT
        localDataService.calibrationIndexer(request.useLiteMode, "stateId")
        ans = localDataService.createCalibrationRecord(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START()


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
        record.indexEntry.version = 1
        record.calculationParameters.version = 1
        with state_root_redirect(localDataService):
            entry = entryFromRecord(record)
            localDataService.writeCalibrationIndexEntry(entry)
            localDataService.writeCalibrationRecord(record)
            actualRecord = localDataService.readCalibrationRecord("57514", useLiteMode, "stateId")
        assert actualRecord.version == record.version
        assert actualRecord == record


def test_writeCalibrationWorkspaces(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    version = randint(2, 120)
    localDataService = LocalDataService()
    stateId = ENDURING_STATE_ID
    testCalibrationRecord = DAOFactory.calibrationRecord("57514", True, 1)
    with state_root_redirect(localDataService, stateId=stateId):
        basePath = localDataService.calibrationIndexer(True, "stateId").versionPath(1)

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
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        create_per_test_sample_workspace(
            outputDSPWSName, detectorState1, fakeInstrumentFilePath, runNumber, units="DSP"
        )
        assert mtd.doesExist(outputDSPWSName)

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


def test_copyCalibration():
    # create base calibration folder for copying
    localDataService = LocalDataService()
    localDataService.generateStateId = mock.Mock(return_value=("stateId", None))
    for useLiteMode in [True, False]:
        record = DAOFactory.calibrationRecord("57514", useLiteMode, version=1)
        with state_root_redirect(localDataService):
            entry = entryFromRecord(record)
            localDataService.writeCalibrationIndexEntry(entry)
            localDataService.writeCalibrationRecord(record)
            # now use python to copy the avove folder
            import shutil

            stateId = ENDURING_STATE_ID
            stateRoot = localDataService.constructCalibrationStateRoot()
            altStateRoot = Resource.getPath("outputs/myAltState")

            # create dummy workspace files in stateRoot
            basePath = localDataService.calibrationIndexer(useLiteMode, "stateId").versionPath(1)
            for workspaceType, workspaceNames in record.workspaces.items():
                for wsName in workspaceNames:
                    if workspaceType == wngt.DIFFCAL_OUTPUT:
                        ext = Config["calibration.diffraction.output.extension"]
                    elif workspaceType == wngt.DIFFCAL_DIAG:
                        ext = Config["calibration.diffraction.diagnostic.extension"]
                    elif workspaceType == wngt.DIFFCAL_TABLE:
                        ext = ".h5"
                    elif workspaceType == wngt.DIFFCAL_MASK:
                        continue

                    wsPath = basePath / (wsName + ext)
                    wsPath.touch()
            orig_constructCalibrationStatePath = localDataService._constructCalibrationStatePath
            try:
                shutil.copytree(stateRoot, altStateRoot)
                liteOrNative = "lite" if useLiteMode else "native"
                assert (Path(altStateRoot) / liteOrNative / "diffraction" / "v_0001").exists()
                assert not (Path(altStateRoot) / liteOrNative / "diffraction" / "v_0002").exists()
                # now that there is a compatible state, try to copy a calibration from the original to the new state
                newEntry = entryFromRecord(DAOFactory.calibrationRecord("57514", useLiteMode, version=2))

                localDataService._constructCalibrationStatePath = mock.Mock()

                def correctStatePath(stateId, _):
                    if stateId == "myAltState":
                        return Path(altStateRoot) / liteOrNative / "diffraction"
                    return Path(stateRoot) / liteOrNative / "diffraction"

                localDataService._constructCalibrationStatePath.side_effect = correctStatePath

                localDataService.copyCalibration(stateId, "myAltState", newEntry)
                # now check that the new entry exists in the new state

                assert (Path(altStateRoot) / liteOrNative / "diffraction" / "v_0002").exists()
                assert (
                    Path(altStateRoot) / liteOrNative / "diffraction" / "v_0002" / "_dsp_column_057514_v0002.nxs.h5"
                ).exists()

            finally:
                localDataService._constructCalibrationStatePath = orig_constructCalibrationStatePath
                shutil.rmtree(altStateRoot)
                pass


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
        localDataService.normalizationIndexer(request.useLiteMode, "stateId")
        ans = localDataService.createNormalizationIndexEntry(request)
        # Set to next version, which on the first call should be the start version
        assert ans.version == VERSION_START()


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
        assert ans.version == VERSION_START()


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
        currentVersion = randint(VERSION_START(), 120)
        runNumber = record.runNumber
        record.version = currentVersion
        record.indexEntry.version = currentVersion
        record.calculationParameters.version = currentVersion
        # NOTE redirect nested so assertion occurs outside of redirect
        # failing assertions inside tempdirs can create unwanted files
        with state_root_redirect(localDataService):
            localDataService.writeNormalizationRecord(record)
            indexer = localDataService.normalizationIndexer(useLiteMode, "stateId")

            indexer.index = {currentVersion: mock.MagicMock(appliesTo=runNumber, version=currentVersion)}
            actualRecord = localDataService.readNormalizationRecord(runNumber, useLiteMode, "stateId", currentVersion)
        assert actualRecord.version == record.version
        assert actualRecord.version == actualRecord.indexEntry.version
        assert record.indexEntry.version == record.version
        assert actualRecord.calculationParameters.version == record.calculationParameters.version
        assert actualRecord == record


def test_writeNormalizationWorkspaces(cleanup_workspace_at_exit):
    version = randint(2, 120)
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    testNormalizationRecord = DAOFactory.normalizationRecord(version=version)
    with (
        mock.patch.object(localDataService, "generateInstrumentState") as mockGenerateInstrumentState,
        state_root_redirect(localDataService, stateId=stateId),
    ):
        mockGenerateInstrumentState.return_value = mock.Mock(
            spec=InstrumentState,
            id=mock.Mock(spec=ObjectSHA, hex=ENDURING_STATE_ID),
            particleBounds=mock.Mock(spec=ParticleBounds, tof=Limit(minimum=0.001, maximum=200000.0)),
        )

        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        newWorkspaceNames = []
        for ws in testNormalizationRecord.workspaceNames:
            newWorkspaceNames.append(ws + "_" + wnvf.formatVersion(version))
        testNormalizationRecord.workspaceNames = newWorkspaceNames
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

        basePath = localDataService.normalizationIndexer(useLiteMode, "stateId").versionPath(version)

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


def test_writeNormalizationWorkspaces_event_binning(cleanup_workspace_at_exit):
    version = randint(2, 120)
    stateId = ENDURING_STATE_ID
    localDataService = LocalDataService()
    testNormalizationRecord = DAOFactory.normalizationRecord(version=version)
    with (
        mock.patch.object(localDataService, "generateInstrumentState") as mockGenerateInstrumentState,
        Config_override("nexus.dataFormat.event", True),
        state_root_redirect(localDataService, stateId=stateId),
    ):
        mockGenerateInstrumentState.return_value = mock.Mock(
            spec=InstrumentState,
            id=mock.Mock(spec=ObjectSHA, hex=ENDURING_STATE_ID),
            particleBounds=mock.Mock(spec=ParticleBounds, tof=Limit(minimum=0.001, maximum=200000.0)),
        )

        # Workspace names need to match the names that are used in the test record.
        runNumber = testNormalizationRecord.runNumber  # noqa: F841
        useLiteMode = testNormalizationRecord.useLiteMode
        newWorkspaceNames = []
        for ws in testNormalizationRecord.workspaceNames:
            newWorkspaceNames.append(ws + "_" + wnvf.formatVersion(version))
        testNormalizationRecord.workspaceNames = newWorkspaceNames
        testWS0, testWS1, testWS2 = testNormalizationRecord.workspaceNames

        basePath = localDataService.normalizationIndexer(useLiteMode, "stateId").versionPath(version)

        # Create sample workspaces containing event data.
        CreateSampleWorkspace(
            OutputWorkspace=testWS0,
            WorkspaceType="Event",
            Function="Powder Diffraction",
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=testWS0,
            Filename=fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )
        # Verify that the original sample workspace is non-trivially binned.
        assert len(mtd[testWS0].readX(0)) > 2

        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS1)
        CloneWorkspace(InputWorkspace=testWS0, OutputWorkspace=testWS2)
        assert mtd.doesExist(testWS0)
        cleanup_workspace_at_exit(testWS0)
        assert mtd.doesExist(testWS1)
        cleanup_workspace_at_exit(testWS1)
        assert mtd.doesExist(testWS2)
        cleanup_workspace_at_exit(testWS2)

        localDataService.writeNormalizationWorkspaces(testNormalizationRecord)

        # Verify that each reloaded workspace is trivially binned.
        for wsName in testNormalizationRecord.workspaceNames:
            filePath = basePath / Path(wsName + ".nxs")
            reloadedWsName = wsName + "_reloaded"
            assert (filePath).exists()
            LoadNexusProcessed(FileName=str(filePath), OutputWorkspace=reloadedWsName)
            assert mtd.doesExist(reloadedWsName)
            cleanup_workspace_at_exit(reloadedWsName)
            ws_ = mtd[reloadedWsName]
            maxBinEdges = max([len(ws_.readX(n)) for n in range(ws_.getNumberHistograms())])
            assert maxBinEdges == 2


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
        # sleep to ensure the timestamp is different
        time.sleep(1)
        actualRecord = localDataService.readReductionRecord(runNumber, useLiteMode, newTimestamp)
        assert testReductionRecord_v0002.timestamp == newTimestamp
        assert datetime.fromtimestamp(actualRecord.timestamp) == datetime.fromtimestamp(newTimestamp)


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
        record = ReductionRecord(**dict_)
        record.workspaceNames = wss

        return record

    yield _readSyntheticReductionRecord

    # teardown...
    pass


@pytest.fixture
def createReductionWorkspaces(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    # Create sample workspaces from a list of names:
    #   * these workspaces are automatically deleted at teardown.

    def _createWorkspaces(wss: List[WorkspaceName]):
        # Create several sample reduction event workspaces with DSP units
        src = mtd.unique_hidden_name()
        runNumber = "12345"
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        create_per_test_sample_workspace(src, detectorState1, fakeInstrumentFilePath, runNumber, units="DSP")

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

        DeleteWorkspaces(
            [
                mask,
            ]
        )  # 'src' marked for deletion by `cleanup_workspace_at_exit`
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
        localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
            localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
            localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
        localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
    testRecord: ReductionRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
            assert "indexEntry" in dict_["calibration"]

            actualRecord = parse_obj_as(ReductionRecord, dict_)
            assert actualRecord == testRecord


def test_readWriteReductionData(readSyntheticReductionRecord, createReductionWorkspaces, cleanup_workspace_at_exit):
    # In order to facilitate parallel testing: any workspace name used by this test should be unique.
    _uniquePrefix = "_test_RWRD_"
    inputRecordFilePath = Path(Resource.getPath("inputs/reduction/ReductionRecord_20240614T130420.json"))
    _uniqueTimestamp = 1718909801.915520
    testRecord = readSyntheticReductionRecord(inputRecordFilePath, _uniqueTimestamp)

    runNumber, useLiteMode, timestamp = testRecord.runNumber, testRecord.useLiteMode, testRecord.timestamp
    stateId = ENDURING_STATE_ID
    fileName = wng.reductionOutputGroup().runNumber(runNumber).timestamp(timestamp).build()
    fileName += Config["reduction.output.extension"]

    wss = createReductionWorkspaces(testRecord.workspaceNames)  # noqa: F841
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId):
        localDataService._instrumentConfig = mock.Mock()
        localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
        assert actualRecord.timestamp == testRecord.timestamp
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
            localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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

            assert actualRecord.timestamp == testRecord.timestamp

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
        localDataService.getIPTS = mock.Mock(return_value=Path("IPTS-12345"))

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
    testIPTS = Path("IPTS-12345")
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
        localDataService.readCalibrationState("123", True, "stateId")


def test_readCalibrationState_hasWritePermissions():
    localDataService = LocalDataService()
    localDataService.calibrationExists = mock.Mock(return_value=False)
    localDataService._hasWritePermissionsCalibrationStateRoot = mock.Mock(return_value=True)

    with pytest.raises(RecoverableException, match="State uninitialized"):
        localDataService.readCalibrationState("123", True, "stateId")


@mock.patch("snapred.backend.data.GroceryService.GroceryService.createDiffCalTableWorkspaceName")
@mock.patch("snapred.backend.data.GroceryService.GroceryService._fetchInstrumentDonor")
def test_writeDefaultDiffCalTable(fetchInstrumentDonor, createDiffCalTableWorkspaceName):
    # verify that the default diffcal table is being written to the default state directory
    runNumber = "default"
    version = VERSION_START()
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
        # NOTE: the above method produces no record
        file = localDataService.calibrationIndexer(useLiteMode, "stateId").versionPath(version) / wsName
        file = file.with_suffix(".h5")
        assert file.exists()


def test_readWriteNormalizationState():
    # NOTE this test is already covered by tests of the Indexer
    # but it doesn't hurt to retain this test anyway
    runNumber = "123"
    useLiteMode = True
    localDataService = LocalDataService()
    latestVersion = 1
    nextVersion = 2

    mockNormalizationIndexer = mock.Mock()
    mockNormalizationIndexer.latestApplicableVersion = mock.Mock(return_value=latestVersion)
    mockNormalizationIndexer.nextVersion = mock.Mock(return_value=nextVersion)

    localDataService.generateStateId = mock.Mock(return_value=("123", "456"))

    localDataService.normalizationIndexer = mock.Mock(return_value=mockNormalizationIndexer)

    mockNormalization = mock.Mock(spec=Normalization, seedRun=runNumber, useLiteMode=useLiteMode)

    localDataService.writeNormalizationState(mockNormalization)
    localDataService.normalizationIndexer.assert_called_once_with(mockNormalization.useLiteMode, "123")
    mockNormalizationIndexer.writeParameters.assert_called_once_with(mockNormalization)
    localDataService.normalizationIndexer.reset_mock()
    mockNormalizationIndexer.reset_mock()

    actual = localDataService.readNormalizationState(runNumber, True, VersionState.LATEST)
    assert actual == mockNormalizationIndexer.readParameters.return_value
    mockNormalizationIndexer.readParameters.assert_called_once_with(latestVersion)


def test_readDetectorState():
    # Verify that `readDetectorState` uses `readRunMetadata`.
    runNumber = "123"
    expected = mockDetectorState("123")
    mockMetadata = mock.Mock(spec=RunMetadata, runNumber=runNumber, detectorState=expected)
    instance = LocalDataService()
    instance.readRunMetadata = mock.Mock(return_value=mockMetadata)
    actual = instance.readDetectorState(runNumber)
    assert actual == expected
    instance.readRunMetadata.assert_called_once_with(runNumber)


@mock.patch(ThisService + "MantidSnapper")
def test__readLiveData(mockSnapper):
    now_ = datetime.utcnow()
    duration = 42
    # 42 seconds ago
    now_minus_42 = now_ + timedelta(seconds=-duration)
    expectedStartTime = now_minus_42.isoformat()
    testWs = "testWs"

    with mock.patch(ThisService + "datetime", wraps=datetime) as mockDatetime:
        mockDatetime.utcnow.return_value = now_

        instance = LocalDataService()
        result = instance._readLiveData(testWs, duration)
        mockSnapper.return_value.LoadLiveData.assert_called_with(
            "load live-data chunk",
            OutputWorkspace=testWs,
            Instrument=Config["liveData.instrument.name"],
            AccumulationMethod=Config["liveData.accumulationMethod"],
            StartTime=expectedStartTime,
            PreserveEvents=False,
        )
        mockSnapper.return_value.executeQueue.assert_called_once()
        assert result == testWs


@mock.patch(ThisService + "MantidSnapper")
def test__readLiveData_from_now(mockSnapper):
    duration = 0
    # _zero_ is a special case => read from _now_: <StartTime: EPOCH_ZERO>
    expectedStartTime = RunMetadata.FROM_NOW_ISO8601
    testWs = "testWs"

    instance = LocalDataService()
    result = instance._readLiveData(testWs, duration)  # noqa: F841
    mockSnapper.return_value.LoadLiveData.assert_called_with(
        "load live-data chunk",
        OutputWorkspace=testWs,
        Instrument=Config["liveData.instrument.name"],
        AccumulationMethod=Config["liveData.accumulationMethod"],
        StartTime=expectedStartTime,
        PreserveEvents=False,
    )


@mock.patch(ThisService + "RunMetadata")
@mock.patch(ThisService + "MantidSnapper")
def test_readLiveMetadata(mockSnapper, mockRunMetadata):
    testWs = "testWs"
    mockWs = mock.Mock()
    mockRun = mock.Mock(
        spec=Run,
        hasProperty=mock.Mock(return_value=True),
        getProperty=mock.Mock(return_value=mock.Mock(value=mock.sentinel.runNumber)),
    )
    mockWs.getRun.return_value = mockRun
    mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=mock.sentinel.stateIdSchema)

    mockSnapper.return_value.mtd.unique_hidden_name.return_value = testWs
    mockSnapper.return_value.mtd.__getitem__.return_value = mockWs

    mockRunMetadata.fromRun = mock.Mock(return_value=mock.sentinel.metadata)

    instance = LocalDataService()
    with (
        mock.patch.object(instance, "_readLiveData") as mock__readLiveData,
        mock.patch.object(instance, "readInstrumentConfig") as mock_readInstrumentConfig,
    ):
        mock__readLiveData.return_value = testWs
        mock_readInstrumentConfig.return_value = mockInstrumentConfig

        actual = instance.readLiveMetadata()

        mock__readLiveData.assert_called_once_with(testWs, duration=0)
        mock_readInstrumentConfig.assert_called_once_with(mock.sentinel.runNumber)
        mockRunMetadata.fromRun.assert_called_once_with(mockRun, mock.sentinel.stateIdSchema, liveData=True)
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


def test_stateIdFromWorkspace(instrumentWorkspace):
    service = LocalDataService()
    runNumber = "12345"
    detectorState1 = mockDetectorState(runNumber)
    wsName = instrumentWorkspace

    # --- duplicates `groceryService.updateInstrumentParameters`: -----
    logsInfo = getInstrumentLogDescriptors(detectorState1)
    addInstrumentLogs(wsName, **logsInfo)
    #  For good measure, we also add the 'run_number' and 'run_title' logs:
    logs = mtd[wsName].mutableRun()
    logs.addProperty("run_number", runNumber, True)
    logs.addProperty("run_title", f"ws for {runNumber}", True)
    # ------------------------------------------------------

    SHA = DetectorState.fromPVLogs(detectorState1.toPVLogs(), DetectorState.LEGACY_SCHEMA).stateId
    expected = SHA.hex, detectorState1

    with mock.patch.object(service, "readInstrumentParameters") as mockReadInstrumentParameters:
        mockReadInstrumentParameters.return_value = mock.Mock(
            spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA
        )
        actual = service.stateIdFromWorkspace(wsName)
        assert actual == expected


@mock.patch(ThisService + "RunMetadata")
def test_initializeState(mockRunMetadata):
    # Test 'initializeState'; test basic functionality.
    runNumber = "123"
    useLiteMode = True
    stateId, detectorState = mockGenerateStateId(runNumber)
    mockRunMetadata.fromNeXusLogs.return_value = RunMetadata.model_construct(
        runNumber=runNumber, detectorState=detectorState, stateId=stateId
    )

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock(return_value=mockPVFile(detectorState))
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = DAOFactory.calibrationParameters(
        runNumber=runNumber,
        useLiteMode=useLiteMode,
        version=VERSION_START(),
        instrumentState=DAOFactory.pv_instrument_state.model_copy(),
    )

    localDataService._instrumentConfig = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._prepareStateRoot = mock.Mock()
    localDataService.readInstrumentConfig = mock.MagicMock(
        return_value=testCalibrationData.instrumentState.instrumentConfig
    )

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = stateId.hex
        stateRootPath = Path(tmpDir) / stateId
        localDataService.constructCalibrationStateRoot = mock.Mock(return_value=stateRootPath)

        actual = localDataService.initializeState(runNumber, useLiteMode, "test")
        actual.creationDate = testCalibrationData.creationDate

    testCalibrationData.indexEntry.comments = (
        "The default configuration when loading StateConfig if none other is found"
    )
    testCalibrationData.indexEntry.author = "SNAPRed Internal"
    testCalibrationData.indexEntry.appliesTo = ">=0"
    testCalibrationData.indexEntry.version = VERSION_START()
    # this is generate at runtime of the test, so just overwrite it.
    testCalibrationData.indexEntry.timestamp = actual.indexEntry.timestamp

    assert actual == testCalibrationData
    assert localDataService._writeDefaultDiffCalTable.call_count == 2
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, True)
    localDataService._writeDefaultDiffCalTable.assert_any_call(runNumber, False)


@mock.patch(ThisService + "RunMetadata")
def test_initializeState_calls_prepareStateRoot(mockRunMetadata):
    # Test that 'initializeState' initializes the <state root> directory.

    runNumber = "123"
    useLiteMode = True
    stateId, detectorState = mockGenerateStateId(runNumber)
    mockRunMetadata.fromNeXusLogs.return_value = RunMetadata.model_construct(
        runNumber=runNumber, detectorState=detectorState, stateId=stateId
    )

    localDataService = LocalDataService()
    localDataService._readPVFile = mock.Mock(return_value=mockPVFile(detectorState))
    localDataService._writeDefaultDiffCalTable = mock.Mock()

    testCalibrationData = DAOFactory.calibrationParameters()
    localDataService._instrumentConfig = testCalibrationData.instrumentState.instrumentConfig
    localDataService.writeCalibrationState = mock.Mock()
    localDataService._readDefaultGroupingMap = mock.Mock(return_value=mock.Mock(isDirty=False))
    localDataService.generateInstrumentState = mock.MagicMock(return_value=testCalibrationData.instrumentState)
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        stateId = stateId.hex
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
    with (
        Config_override("instrument.home", "this/path/does/not/exist"),
        Config_override("localdataservice.config.verifypaths", True),
    ):
        with pytest.raises(FileNotFoundError):
            service.readInstrumentConfig(12345)


def test_noInstrumentConfig():
    """This verifies that a broken configuration (from production) can't find all of the files"""
    # get a handle on the service
    service = LocalDataService()
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
        with (
            Config_override("instrument.parameters.home", str(tempdir)),
            Config_override("localdataservice.config.verifypaths", True),
        ):
            with pytest.raises(FileNotFoundError):
                service.readInstrumentConfig(12345)


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


def test_writeDiffCalWorkspaces(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        tableWSName = "test_table"
        maskWSName = "test_mask"
        filename = Path(tableWSName + ".h5")

        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        runNumber = "12345"
        create_per_test_sample_workspace(instrumentDonor, detectorState1, fakeInstrumentFilePath, runNumber)
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


def test_writeDiffCalWorkspaces_mask_only(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        maskWSName = "test_mask"
        filename = Path(maskWSName + ".h5")

        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        runNumber = "12345"
        create_per_test_sample_workspace(instrumentDonor, detectorState1, fakeInstrumentFilePath, runNumber)
        assert mtd.doesExist(instrumentDonor)

        # Create mask workspace to write.
        createCompatibleMask(maskWSName, instrumentDonor)
        cleanup_workspace_at_exit(maskWSName)
        assert mtd.doesExist(maskWSName)
        localDataService.writeDiffCalWorkspaces(basePath, filename, maskWorkspaceName=maskWSName)
        assert (basePath / filename).exists()


def test_writeDiffCalWorkspaces_bad_path(cleanup_workspace_at_exit, create_per_test_sample_workspace):
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
            detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
            runNumber = "12345"
            create_per_test_sample_workspace(instrumentDonor, detectorState1, fakeInstrumentFilePath, runNumber)
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


def test_writePixelMask(cleanup_workspace_at_exit, create_per_test_sample_workspace):
    localDataService = LocalDataService()
    path = Resource.getPath("outputs")
    with tempfile.TemporaryDirectory(dir=path, suffix=os.sep) as basePath:
        basePath = Path(basePath)
        maskWSName = "test_mask"
        filename = Path(maskWSName + ".h5")

        # Create an instrument workspace.
        instrumentDonor = "test_instrument_donor"
        detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        runNumber = "12345"
        create_per_test_sample_workspace(instrumentDonor, detectorState1, fakeInstrumentFilePath, runNumber)
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
        cls.runNumber5 = "123460"
        cls.runNumber6 = "123470"
        cls.useLiteMode = True

        cls.timestamp1 = cls.service.getUniqueTimestamp()
        cls.timestamp2 = cls.service.getUniqueTimestamp()
        cls.timestamp3 = cls.service.getUniqueTimestamp()
        cls.timestamp4 = cls.service.getUniqueTimestamp()

        # Arbitrary, but distinct, `DetectorState`s used for realistic instrument initialization
        cls.detectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        cls.detectorState2 = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))
        cls.detectorState3 = DetectorState(arc=(7.0, 9.0), wav=9.0, freq=11.0, guideStat=2, lin=(11.0, 13.0))

        # The corresponding stateId:
        cls.stateId1 = DetectorState.fromPVLogs(cls.detectorState1.toPVLogs(), DetectorState.LEGACY_SCHEMA).stateId.hex
        cls.stateId2 = DetectorState.fromPVLogs(cls.detectorState2.toPVLogs(), DetectorState.LEGACY_SCHEMA).stateId.hex
        cls.stateId3 = DetectorState.fromPVLogs(cls.detectorState3.toPVLogs(), DetectorState.LEGACY_SCHEMA).stateId.hex

        cls.instrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
        cls.instrumentLiteFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPLite.xml")
        instrumentFilePath = cls.instrumentLiteFilePath if cls.useLiteMode else cls.instrumentFilePath

        # create instrument workspaces for each state
        cls.sampleWS1 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS1, cls.detectorState1, instrumentFilePath, cls.runNumber1)
        cls.sampleWS2 = mtd.unique_hidden_name()
        create_sample_workspace(cls.sampleWS2, cls.detectorState2, instrumentFilePath, cls.runNumber2)

        with mock.patch.object(cls.service, "readInstrumentParameters") as mockReadInstrumentParameters:
            mockReadInstrumentParameters.return_value = mock.Mock(
                spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA
            )
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

        with mock.patch.object(cls.service, "readInstrumentParameters") as mockReadInstrumentParameters:
            mockReadInstrumentParameters.return_value = mock.Mock(
                spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA
            )
            assert cls.service.stateIdFromWorkspace(cls.maskWS1)[0] == cls.stateId1
            assert cls.service.stateIdFromWorkspace(cls.maskWS2)[0] == cls.stateId2
            assert cls.service.stateIdFromWorkspace(cls.maskWS3)[0] == cls.stateId1
            assert cls.service.stateIdFromWorkspace(cls.maskWS4)[0] == cls.stateId2
        yield

        # teardown...
        pass

    def _createReductionFileSystem(self):
        assert self.timestamp1 != self.timestamp2
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
                self.runNumber1: (self.stateId1, self.detectorState1),
                self.runNumber2: (self.stateId1, self.detectorState1),
                self.runNumber3: (self.stateId2, self.detectorState2),
                self.runNumber4: (self.stateId2, self.detectorState2),
                self.runNumber5: (self.stateId3, None),  # Deliberate: no non live-data detector state for this run.
                self.runNumber6: (self.stateId3, None),  # Deliberate: no non live-data detector state for this run.
            }[runNumber],
        )

        def mockGetIPTS(runNumber, _instrumentName="SNAP"):
            if runNumber == self.runNumber6:
                # This tests that any error, besides not finding the IPTS-directory, is not swallowed or relabeled.
                raise RuntimeError("Some other runtime error")
            else:
                return {
                    self.runNumber1: Path("/SNS/SNAP/IPTS-1"),
                    self.runNumber2: Path("/SNS/SNAP/IPTS-1"),
                    self.runNumber3: Path("/SNS/SNAP/IPTS-2"),
                    self.runNumber4: Path("/SNS/SNAP/IPTS-2"),
                    self.runNumber5: None,  # Deliberate: no IPTS-directory exists for this run.
                }[runNumber]

        monkeypatch.setattr(self.service, "getIPTS", mockGetIPTS)

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

    def test__reducedRuns_no_IPTS(self):
        # Verify the negative case that no IPTS directory generates an empty run numbers list.

        # NOTE: other aspects of `_reducedRuns` are checked implicitly in the previous tests.
        #   For this test we need to _bypass_ most of the default setup.

        # `self.runNumber5` is special for this test.

        runs = self.service._reducedRuns(self.runNumber5, True)
        assert runs == []

    def test__reducedRuns_other_runtime_error(self):
        with Config_override("instrument.reduction.home", "a.string.with.{IPTS}.in.it"):
            with pytest.raises(RuntimeError, match="Some other runtime error"):
                self.service._reducedRuns(self.runNumber6, True)  # noqa: F841

    @mock.patch(ThisService + "Path")
    def test_findCompatibleStates(self, mockPath):
        compatibleDetectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        compatibleDetectorState2 = DetectorState(
            arc=[compatibleDetectorState1["det_arc1"], compatibleDetectorState1["det_arc2"]],
            wav=9.0,
            freq=10.0,
            guideStat=compatibleDetectorState1["BL3:Mot:OpticsPos:Pos"],
            lin=(11.0, 12.0),
        )
        incompatibleDetectorState = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

        pathTuple = namedtuple("Path", ["name", "is_dir"])

        mockPath().iterdir.return_value = [
            pathTuple(name="123456(comp)", is_dir=lambda: True),
            pathTuple(name="123457(comp)", is_dir=lambda: True),
            pathTuple(name="123458(incomp)", is_dir=lambda: True),
        ]

        mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA)

        mockIndexer = mock.Mock()
        self.service.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        mockIndexer.defaultVersion.return_value = "-1"
        mockIndexer.currentVersion.return_value = "0"
        mockIndexer.latestApplicableVersion.return_value = "0"
        mockIndexer.readParameters.side_effect = [
            mock.Mock(
                instrumentState=mock.Mock(
                    spec=InstrumentState, instrumentConfig=mockInstrumentConfig, detectorState=compatibleDetectorState1
                )
            ),
            mock.Mock(
                instrumentState=mock.Mock(
                    spec=InstrumentState, instrumentConfig=mockInstrumentConfig, detectorState=compatibleDetectorState2
                )
            ),
            mock.Mock(
                instrumentState=mock.Mock(
                    spec=InstrumentState, instrumentConfig=mockInstrumentConfig, detectorState=incompatibleDetectorState
                )
            ),
        ]

        self.service.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)
        self.service.readDetectorState = mock.Mock(return_value=compatibleDetectorState1)

        result = self.service.findCompatibleStates("123", True)
        assert len(result) == 2
        assert "123456(comp)" in result
        assert "123457(comp)" in result
        assert "123458(incomp)" not in result

    @mock.patch(ThisService + "Path")
    def test_findCompatibleStates_butLacksCalibration(self, mockPath):
        compatibleDetectorState1 = DetectorState(arc=(1.0, 2.0), wav=3.0, freq=4.0, guideStat=1, lin=(5.0, 6.0))
        incompatibleDetectorState = DetectorState(arc=(7.0, 8.0), wav=9.0, freq=10.0, guideStat=2, lin=(11.0, 12.0))

        pathTuple = namedtuple("Path", ["name", "is_dir"])

        mockPath().iterdir.return_value = [
            pathTuple(name="123456(comp)", is_dir=lambda: True),
            pathTuple(name="123457(comp)", is_dir=lambda: True),
            pathTuple(name="123458(incomp)", is_dir=lambda: True),
        ]

        mockInstrumentConfig = mock.Mock(spec=InstrumentConfig, stateIdSchema=DetectorState.LEGACY_SCHEMA)

        mockIndexer = mock.Mock()
        self.service.calibrationIndexer = mock.Mock(return_value=mockIndexer)
        mockIndexer.defaultVersion.return_value = "-1"
        mockIndexer.currentVersion.side_effect = ["0", "-1", "0"]
        mockIndexer.latestApplicableVersion.return_value = "0"
        mockIndexer.readParameters.side_effect = [
            mock.Mock(
                instrumentState=mock.Mock(
                    spec=InstrumentState, instrumentConfig=mockInstrumentConfig, detectorState=compatibleDetectorState1
                )
            ),
            mock.Mock(
                instrumentState=mock.Mock(
                    spec=InstrumentState, instrumentConfig=mockInstrumentConfig, detectorState=incompatibleDetectorState
                )
            ),
        ]

        self.service.readInstrumentConfig = mock.Mock(return_value=mockInstrumentConfig)
        self.service.readDetectorState = mock.Mock(return_value=compatibleDetectorState1)

        result = self.service.findCompatibleStates("123", True)
        assert len(result) == 1
        assert "123456(comp)" in result
        assert "123457(comp)" not in result
        assert "123458(incomp)" not in result

    def generateFakeCalibrationRoot(self, root):
        # Create a fake calibration root directory structure
        (root / "CalibrationRoot").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "CalibrantSamples").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "CalibrantSamples" / ".git").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "Powder").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "SNAPInstPrm").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "Powder" / "PixelGroupingDefinitions").mkdir(parents=True, exist_ok=True)
        (root / "CalibrationRoot" / "Powder" / "SNAPLite.xml").touch()
        (root / "CalibrationRoot" / "Powder" / ".mysecretcode").touch()

    def test_copyCalibrationRootSkeleton(self):
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
            tmpDir = Path(tmpDir)
            # copy it to the temporary directory
            self.generateFakeCalibrationRoot(tmpDir)
            self.service.copyCalibrationRootSkeleton(tmpDir / "CalibrationRoot", tmpDir / "Calibration")
            assert (tmpDir / "Calibration").exists()
            assert (tmpDir / "Calibration" / "CalibrantSamples").exists()
            assert (tmpDir / "Calibration" / "Powder").exists()
            assert (tmpDir / "Calibration" / "Powder" / "PixelGroupingDefinitions").exists()
            assert (tmpDir / "Calibration" / "Powder" / "SNAPLite.xml").exists()
            # ignore hidden files
            assert not (tmpDir / "Calibration" / "Powder" / ".mysecretcode").exists()
            # ignore hidden dirs
            assert not (tmpDir / "Calibration" / "CalibrantSamples" / ".git").exists()

            with mock.patch(ThisService + "logger") as mockLogger:
                self.service.copyCalibrationRootSkeleton(tmpDir / "CalibrationRoot", tmpDir / "Calibration")
                assert mockLogger.info.call_count == 1
                assert "already exists" in mockLogger.info.call_args[0][0]
                assert "Calibration" in mockLogger.info.call_args[0][0]

    def test_copyCalibrationRootSkeleton_missing_calibration_root_data(self):
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
            tmpDir = Path(tmpDir)
            with pytest.raises(FileNotFoundError, match="Failure to find"):
                self.service.copyCalibrationRootSkeleton(tmpDir, tmpDir / "Calibration")

    def test_generateUserRootFolder(self):
        # mock out config
        with (
            mock.patch(ThisService + "Config", mock.MagicMock()) as mockConfig,
            mock.patch(ThisService + "LocalDataService.copyCalibrationRootSkeleton", mock.Mock()),
        ):
            # mock out the path
            self.service.generateUserRootFolder()
            assert self.service.copyCalibrationRootSkeleton.called
            assert self.service.copyCalibrationRootSkeleton.call_count == 1
            assert mockConfig.__getitem__.called


### GENERALIZED PROGRESS REPORTING:


class TestProgressRecordingMethods:
    @pytest.fixture(autouse=True)
    def _setup_tests(self):
        # setup
        self.instance = LocalDataService()
        yield

        # teardown
        pass

    def test__progressRecordsFilenameStem(self):
        stem = self.instance._progressRecordsFilenameStem()
        allowedCharacters = re.compile(r"\w.*")  # "[A-Za-z0-9_]"
        assert isinstance(stem, str)
        assert allowedCharacters.match(stem)

    def test_progressRecordsPath(self):
        userApplicationDataHome = Path("<user_home>/.snapred")
        with Config_override("user.application.data.home", str(userApplicationDataHome)):
            recordsPath = self.instance._progressRecordsPath()
            assert recordsPath == userApplicationDataHome / "workflows_data" / "timing"

    def test__progressRecordsFilePath(self):
        naiveTime = datetime.now()
        timeWithTimeZone = naiveTime.astimezone(timezone.utc)

        # provides expected path with non-naive timestamp
        with (
            mock.patch.object(LocalDataService, "_progressRecordsPath") as mock_progressRecordsPath,
            mock.patch.object(LocalDataService, "_progressRecordsFilenameStem") as mock_progressRecordsFilenameStem,
        ):
            recordsPath = Path("<user_home>/.snapred/workflows_data/timing")
            filenameStem = "execution_timing"
            mock_progressRecordsPath.return_value = recordsPath
            mock_progressRecordsFilenameStem.return_value = filenameStem

            filePath = self.instance._progressRecordsFilePath(timeWithTimeZone)
            assert filePath == recordsPath / (filenameStem + "_" + timeWithTimeZone.isoformat() + ".json")
            mock_progressRecordsPath.assert_called_once()
            mock_progressRecordsFilenameStem.assert_called_once()

        # converts naive to non-naive timestamp using `timezone.utc`
        with (
            mock.patch.object(LocalDataService, "_progressRecordsPath") as mock_progressRecordsPath,
            mock.patch.object(LocalDataService, "_progressRecordsFilenameStem") as mock_progressRecordsFilenameStem,
        ):
            recordsPath = Path("<user_home>/.snapred/workflows_data/timing")
            filenameStem = "execution_timing"
            mock_progressRecordsPath.return_value = recordsPath
            mock_progressRecordsFilenameStem.return_value = filenameStem

            filePath = self.instance._progressRecordsFilePath(naiveTime)
            assert filePath == recordsPath / (filenameStem + "_" + timeWithTimeZone.isoformat() + ".json")

    def test__progressRecordsFilePaths(self):
        # Initialize a progress-records directory containing
        #   files using the expected naming scheme.
        latest = datetime.now(timezone.utc)
        middle = latest - timedelta(seconds=120.0)
        earliest = latest - timedelta(seconds=2 * 120.0)
        # paths in order of their timestamps
        filenames = [
            f"execution_timing_{earliest.isoformat()}.json",
            f"execution_timing_{middle.isoformat()}.json",
            f"execution_timing_{latest.isoformat()}.json",
        ]

        # `_progressRecordsFilePaths` returns a timestamp-sorted list of paths
        with (
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            Config_override("user.application.data.home", str(tempDir)),
        ):
            # create the test files
            recordsPath = Path(tempDir) / "workflows_data" / "timing"
            recordsPath.mkdir(parents=True)
            for name in filenames:
                (recordsPath / name).touch()

            # the paths list should be in timestamp order
            paths = self.instance._progressRecordsFilePaths()
            for n, path in enumerate(paths):
                assert path == recordsPath / filenames[n]

        # `_progressRecordsFilePaths` ignores other files in the same directory
        with (
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            Config_override("user.application.data.home", str(tempDir)),
        ):
            # create the test files
            recordsPath = Path(tempDir) / "workflows_data" / "timing"
            recordsPath.mkdir(parents=True)
            for n, name in enumerate(filenames):
                (recordsPath / name).touch()
                (recordsPath / (name + "junk")).touch()
                (recordsPath / ("junk" + name)).touch()
                (recordsPath / f"junk_{n}.json").touch()

            # the paths list should be the same as before
            paths = self.instance._progressRecordsFilePaths()
            for n, path in enumerate(paths):
                assert path == recordsPath / filenames[n]

    def test_progressRecordsSaveFilePath(self):
        # the path for the current progress-records file is generated using `getUniqueTimestamp`,
        #   and its timestamp will use the UTC timezone
        _now = datetime.now(timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(LocalDataService), "datetime") as mock_datetime,
            mock.patch.object(LocalDataService, "getUniqueTimestamp") as mock_getUniqueTimestamp,
            mock.patch.object(LocalDataService, "_progressRecordsFilePath") as mock_progressRecordsFilePath,
        ):
            mock_datetime.fromtimestamp.return_value = _now
            mock_progressRecordsFilePath.return_value = mock.sentinel.path
            mock_getUniqueTimestamp.return_value = _now.timestamp()

            path = self.instance._progressRecordsSaveFilePath()
            assert path == mock.sentinel.path
            mock_getUniqueTimestamp.assert_called_once()
            mock_datetime.fromtimestamp.assert_called_once_with(mock_getUniqueTimestamp.return_value, tz=timezone.utc)
            mock_progressRecordsFilePath.assert_called_once_with(_now)

    def test_readProgressRecords(self):
        # Initialize a progress-records directory containing
        #   files using the expected naming scheme.
        latest = datetime.now(timezone.utc)
        middle = latest - timedelta(seconds=120.0)
        earliest = latest - timedelta(seconds=2 * 120.0)
        filenames = [
            f"execution_timing_{earliest.isoformat()}.json",
            f"execution_timing_{middle.isoformat()}.json",
            f"execution_timing_{latest.isoformat()}.json",
        ]

        # returns the <json-format string> contents of the latest progress-records data file
        with (
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            Config_override("user.application.data.home", str(tempDir)),
            mock.patch.object(LocalDataService, "_progressRecordsFilePaths") as mock_progressRecordsFilePaths,
        ):
            recordsPath = Path(tempDir) / "workflows_data" / "timing"
            latestFilePath = recordsPath / filenames[-1]
            mock_progressRecordsFilePaths.return_value = [recordsPath / name for name in filenames]

            # create the test files
            recordsPath.mkdir(parents=True)
            for n, name in enumerate(filenames):
                with open(recordsPath / name, "w") as dataFile:
                    # write valid data, ensuring that it is unique
                    dataFile.write(f"{{'steps': [{{'details': {{'key': [one_{n}, two_{n}, three_{n}]}}}}]}}")

            expected = None
            with open(latestFilePath, "r") as dataFile:
                expected = dataFile.read()
            actual = self.instance.readProgressRecords()
            assert actual == expected
            mock_progressRecordsFilePaths.assert_called_once()

        # when no data exists: returns a valid initializer <json-format string> for the `_ProgressRecorder`
        with (
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            Config_override("user.application.data.home", str(tempDir)),
            mock.patch.object(LocalDataService, "_progressRecordsFilePaths") as mock_progressRecordsFilePaths,
        ):
            mock_progressRecordsFilePaths.return_value = []

            expected = '{"steps": []}'
            actual = self.instance.readProgressRecords()
            assert actual == expected
            mock_progressRecordsFilePaths.assert_called_once()

            # make sure it actually works as an initializer string
            recorder = _ProgressRecorder.model_validate_json(actual)
            assert recorder.steps == {}

    def test_writeProgressRecords(self):
        _now = datetime.now()

        # a version of `_progressRecordsFilePath` to be used during this test
        def _recordsFilePath(path: Path, timestamp: datetime) -> Path:
            recordsPath = path / "workflows_data" / "timing"
            return recordsPath / ("execution_timing" + "_" + timestamp.isoformat() + ".json")

        def _fileCount(path: Path):
            _, _, files = next(os.walk(path))
            return len(files)

        # creates the output directory if it doesn't exist and writes the records file
        with (
            Config_override("application.workflows_data.timing.max_files", 3),
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            mock.patch.object(LocalDataService, "_progressRecordsPath") as mock_progressRecordsPath,
            mock.patch.object(LocalDataService, "_progressRecordsSaveFilePath") as mock_progressRecordsSaveFilePath,
            mock.patch.object(LocalDataService, "_progressRecordsFilePaths") as mock_progressRecordsFilePaths,
        ):
            filePath = _recordsFilePath(Path(tempDir), _now)
            recordsPath = filePath.parent
            mock_progressRecordsPath.return_value = recordsPath
            mock_progressRecordsSaveFilePath.return_value = filePath
            mock_progressRecordsFilePaths.return_value = []

            sha = sha256()
            sha.update("Some unique data to save.".encode("utf8"))
            data = sha.hexdigest()

            assert not recordsPath.exists()
            self.instance.writeProgressRecords(data)

            # output directory was created, and the file was written
            assert filePath.exists()

            # the read back data matches the written data
            with open(filePath, "r") as inputData:
                assert inputData.read() == data

            mock_progressRecordsPath.assert_called_once()
            mock_progressRecordsSaveFilePath.assert_called_once()
            mock_progressRecordsFilePaths.assert_called_once()

        # restricts the maximum number of saved records files to the 'max_files' value
        with (
            Config_override("application.workflows_data.timing.max_files", 3),
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempDir,
            mock.patch.object(LocalDataService, "_progressRecordsPath") as mock_progressRecordsPath,
            mock.patch.object(LocalDataService, "_progressRecordsSaveFilePath") as mock_progressRecordsSaveFilePath,
        ):
            filePath = _recordsFilePath(Path(tempDir), _now)
            recordsPath = filePath.parent
            mock_progressRecordsPath.return_value = recordsPath
            mock_progressRecordsSaveFilePath.return_value = filePath

            # create more records files than the 'max_files' limit
            recordsPath.mkdir(parents=True)
            dt = timedelta(seconds=120.0)
            startTime = _now - timedelta(minutes=60.0)
            filePaths = []
            for n in range(2 * Config["application.workflows_data.timing.max_files"]):
                timestamp = startTime + n * dt
                path = _recordsFilePath(Path(tempDir), timestamp)
                path.touch()
                filePaths.append(path)
            assert _fileCount(recordsPath) == len(filePaths)

            # filePaths are already in order, but we will also need to include the
            #   to-be-written path
            mock_progressRecordsFilePaths.return_value = filePaths + [
                filePath,
            ]

            sha = sha256()
            sha.update("Some unique data to save.".encode("utf8"))
            data = sha.hexdigest()
            self.instance.writeProgressRecords(data)

            # output directory was created, and the file was written
            assert filePath.exists()

            # the number of saved files has been restricted as required
            _fileCount(recordsPath) == Config["application.workflows_data.timing.max_files"]

            # only the oldest files were deleted
            latestFiles = filePaths[-Config["application.workflows_data.timing.max_files"] :]
            for path in latestFiles:
                assert path.exists()

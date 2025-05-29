# Unit tests for `tests/util/state_helpers.py`
import inspect
import shutil
from pathlib import Path
from shutil import rmtree

##
## Put test-related imports at the end, so that the normal non-test import sequence is unmodified.
##
from unittest import mock

import numpy as np
import pytest
from util.dao import DAOFactory
from util.h5py_helpers import mockH5File
from util.state_helpers import reduction_root_redirect, state_root_override, state_root_redirect

from snapred.backend.dao.indexing.Versioning import VERSION_START
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf


@pytest.fixture(autouse=True)
def _cleanup_directories():
    stateId = "ab8704b0bc2a2342"
    stateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    yield
    # teardown
    if stateRootPath.exists():
        shutil.rmtree(stateRootPath)


def mockPVFile(detectorState: DetectorState) -> mock.MagicMock:
    DASlogs = {
        "BL3:Chop:Skf1:WavelengthUserReq": np.array([detectorState.wav]),
        "det_arc1": np.array([detectorState.arc[0]]),
        "det_arc2": np.array([detectorState.arc[1]]),
        "BL3:Det:TH:BL:Frequency": np.array([detectorState.freq]),
        "BL3:Mot:OpticsPos:Pos": np.array([detectorState.guideStat]),
        "det_lin1": np.array([detectorState.lin[0]]),
        "det_lin2": np.array([detectorState.lin[1]]),
    }
    specialValues = {
        "run_number": "123456",
        "start_time": "2023-06-14T14:06:40.429048667",
        "end_time": "2023-06-14T14:07:56.123123123",
    }
    return mockH5File(DASlogs, **specialValues)


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "generateStateId")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
@mock.patch.object(inspect.getmodule(RunMetadata), "h5py")
def test_state_root_override_enter(
    mockH5py,
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockGenerateStateId,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # See `test_LocalDataService::test_initializeState`.
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)
    # We also need to mock out the `RunMetadata` "lazy" `h5py.File` descriptor.
    mockH5py.File = mock.Mock(return_value=mockReadPVFile.return_value)

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockGetInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

    stateId = "ab8704b0bc2a2342"
    # NOTE delete the path first or the test can fail for confusing reasons
    expectedStateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    rmtree(expectedStateRootPath, ignore_errors=True)
    detectorState = DAOFactory.unreal_detector_state
    mockGenerateStateId.return_value = (stateId, detectorState)
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True
    with state_root_override(runNumber, stateName, useLiteMode) as stateRootPath:
        assert Path(stateRootPath) == expectedStateRootPath
        assert Path(stateRootPath).exists()
        assert Path(stateRootPath).joinpath("groupingMap.json").exists()
        versionString = wnvf.pathVersion(VERSION_START())
        assert (Path(stateRootPath) / "lite" / "diffraction" / versionString / "CalibrationParameters.json").exists()


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
@mock.patch.object(inspect.getmodule(RunMetadata), "h5py")
def test_state_root_override_exit(
    mockH5py,
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)
    # We also need to mock out the `RunMetadata` "lazy" `h5py.File` descriptor.
    mockH5py.File = mock.Mock(return_value=mockReadPVFile.return_value)

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockGetInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

    stateId = "ab8704b0bc2a2342"  # noqa: F841
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True

    stateRootPath = None
    with state_root_override(runNumber, stateName, useLiteMode) as rootPath:
        stateRootPath = rootPath
        assert Path(stateRootPath).exists()
    assert not Path(stateRootPath).exists()


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "readInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
@mock.patch.object(inspect.getmodule(RunMetadata), "h5py")
def test_state_root_override_exit_no_delete(
    mockH5py,
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)
    # We also need to mock out the `RunMetadata` "lazy" `h5py.File` descriptor.
    mockH5py.File = mock.Mock(return_value=mockReadPVFile.return_value)

    testCalibrationData = DAOFactory.calibrationParameters("57514", True, 1)
    mockGetInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
    mockReadDefaultGroupingMap.return_value = DAOFactory.groupingMap_SNAP()

    stateId = "ab8704b0bc2a2342"  # noqa: F841
    runNumber = "123456"
    stateName = "my happy state"
    useLiteMode = True

    stateRootPath = None
    with state_root_override(runNumber, stateName, useLiteMode, delete_at_exit=False) as rootPath:
        stateRootPath = rootPath
        assert Path(stateRootPath).exists()
    assert Path(stateRootPath).exists()
    shutil.rmtree(stateRootPath)


def test_state_root_redirect_no_stateid():
    localDataService = LocalDataService()
    oldSelf = localDataService.constructCalibrationStateRoot
    with state_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService.constructCalibrationStateRoot() == tmpRoot.path()
        assert localDataService.generateStateId()[0] == tmpRoot.path().parts[-1]
        # make sure a file can be added inside the directory -- can be any file
        # verify it can be found by data services and equals the value written
        localDataService.calibrationExists = mock.Mock(return_value=True)
        expected = DAOFactory.calibrationParameters("xyz", True, 1)
        indexer = localDataService.calibrationIndexer(True, "stateId")
        tmpRoot.saveObjectAt(expected, indexer.parametersPath(1))
        ans = localDataService.readCalibrationState("xyz", True, "stateId", 1)
        assert ans == expected
        # make sure files can only be added inside the directory
        with pytest.raises(AssertionError):
            tmpRoot.saveObjectAt(expected, "here")
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService.constructCalibrationStateRoot


def test_state_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with state_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService.generateStateId() == (stateId, None)
        assert localDataService.constructCalibrationStateRoot() == tmpRoot.path()
        assert stateId == localDataService.constructCalibrationStateRoot().parts[-1]


def test_reduction_root_redirect_no_stateid():
    localDataService = LocalDataService()
    oldSelf = localDataService._constructReductionStateRoot
    with reduction_root_redirect(localDataService) as tmpRoot:
        # make sure the path exists
        assert tmpRoot.path().exists()
        # make sure the data service's path points to the tmp directory
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert localDataService.generateStateId()[0] == tmpRoot.path().parts[-1]
    # make sure the directory is deleted at exit
    assert not tmpRoot.path().exists()
    # make sure the construct state root method is restored on exit
    assert oldSelf == localDataService._constructReductionStateRoot


def test_reduction_root_redirect_with_stateid():
    stateId = "1234567890123456"
    localDataService = LocalDataService()
    with reduction_root_redirect(localDataService, stateId=stateId) as tmpRoot:
        # make sure the root is pointing to this state ID
        assert localDataService.generateStateId() == (stateId, None)
        assert localDataService._constructReductionStateRoot() == tmpRoot.path()
        assert stateId == localDataService._constructReductionStateRoot().parts[-1]

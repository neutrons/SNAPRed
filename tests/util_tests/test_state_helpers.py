# Unit tests for `tests/util/state_helpers.py`
import h5py
from pathlib import Path
import shutil
from shutil import rmtree

from snapred.backend.dao.indexing.Versioning import VERSION_DEFAULT
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf

##
## Put test-related imports at the end, so that the normal non-test import sequence is unmodified.
##
from util.dao import DAOFactory
from util.state_helpers import reduction_root_redirect, state_root_override, state_root_redirect

import unittest.mock as mock
import pytest


@pytest.fixture(autouse=True)
def _cleanup_directories():
    stateId = "ab8704b0bc2a2342"
    stateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    yield
    # teardown
    if stateRootPath.exists():
        shutil.rmtree(stateRootPath)

def mockPVFile(detectorState: DetectorState) -> mock.Mock:
    # See also: `tests/unit/backend/data/util/test_mapping_util.py`.
    
    # Note: `mapping_util.mappingFromNeXusLogs` will open the 'entry/DASlogs' group,
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
    mock_.__getitem__.side_effect =\
       lambda key: mock_ if key == "entry/DASlogs" else dict_[key]
    
    mock_.__contains__.side_effect = dict_.__contains__
    mock_.keys.side_effect = dict_.keys
    return mock_

@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "generateStateId")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "getInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_enter(
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockGenerateStateId,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)

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
        versionString = wnvf.pathVersion(VERSION_DEFAULT)
        assert (Path(stateRootPath) / "lite" / "diffraction" / versionString / "CalibrationParameters.json").exists()


@mock.patch.object(LocalDataService, "_writeDefaultDiffCalTable")
@mock.patch.object(LocalDataService, "_readDefaultGroupingMap")
@mock.patch.object(LocalDataService, "getInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit(
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)

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
@mock.patch.object(LocalDataService, "getInstrumentConfig")
@mock.patch.object(LocalDataService, "_readPVFile")
def test_state_root_override_exit_no_delete(
    mockReadPVFile,
    mockGetInstrumentConfig,
    mockReadDefaultGroupingMap,
    mockWriteDefaultDiffCalTable,  # noqa ARG001
):
    # see `test_LocalDataService::test_initializeState`
    mockReadPVFile.return_value = mockPVFile(DAOFactory.unreal_detector_state)

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
        indexer = localDataService.calibrationIndexer("xyz", True)
        tmpRoot.saveObjectAt(expected, indexer.parametersPath(1))
        ans = localDataService.readCalibrationState("xyz", True, 1)
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

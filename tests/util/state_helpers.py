import shutil
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pydantic import BaseModel

from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Resource
from snapred.meta.redantic import write_model_pretty

# IMPLEMENTATION NOTES:
# * Because so many other required directories are nested under
# `Config['instrument.calibration.powder.home']`, this context manager cannot
# usefully override that directory location; instead, it is limited to creating
# the state-root directory, and initializing the state, and optionally, to deleting these
# directories at exit.
# * No attempt is made to copy an existing state-root directory to a new location;
# if the directory already exists, an exception will be thrown.
# * In order to allow convenient usage within CIS-test scripts,
# `state_root_override` is deliberately _not_ implemented as a test fixture.


@contextmanager
def state_root_override(runNumber: str, name: str, useLiteMode: bool = False, delete_at_exit=True):
    # Context manager to override the state root directory:
    # * creates the state root directory;
    # * initializes the state;
    # * returns the state-root directory path (as `str`).
    # If the state-root directory already exists, an exception is thrown.

    # __enter__
    dataService = LocalDataService()
    stateId, _ = dataService.generateStateId(runNumber)
    stateRoot = Path(dataService.constructCalibrationStateRoot(stateId))
    if stateRoot.exists():
        raise RuntimeError(f"state-root directory '{stateRoot}' already exists -- please move it out of the way!")

    dataService.initializeState(runNumber, useLiteMode, name)
    yield stateRoot

    # __exit__
    if delete_at_exit:
        shutil.rmtree(stateRoot)


@pytest.fixture
def state_root_fixture():
    _stack = ExitStack()

    def _state_root_fixture(runNumber: str, name: str, useLiteMode: bool = False, delete_at_exit=True):
        return _stack.enter_context(state_root_override(runNumber, name, useLiteMode, delete_at_exit))

    yield _state_root_fixture

    # teardown => __exit__
    _stack.close()


class state_root_redirect:
    """
    This context manager will create a temporary directory and patch a LocalDataService so that its
    state root directory points inside the temporary directory.  Files can be easily added to the
    directory using `addFileAs`.  Usage is

    ```
    with state_root_redirect(instance.dataService) as tmpRoot:
        <code here>
        tmpRoot.addFileAs(some_file, target_in_tmp_root)
        <more code here>
    ```
    """

    def __init__(self, dataService: LocalDataService, *, stateId: str = None):
        self.dataService = dataService
        self.stateId = stateId
        self.old_constructCalibrationStateRoot = dataService.constructCalibrationStateRoot
        self.old_generateStateId = dataService.generateStateId

    def __enter__(self):
        self.tmpdir = TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/")
        self.tmppath = Path(self.tmpdir.name)
        if self.stateId is not None:
            self.tmppath = self.tmppath / Path(self.stateId)
        else:
            self.stateId = str(self.tmppath.parts[-1])
        self.dataService.generateStateId = lambda *x, **y: (self.stateId, None)  # noqa ARG005
        self.dataService.constructCalibrationStateRoot = lambda *x, **y: self.tmppath  # noqa ARG005
        return self

    def __exit__(self, *arg, **kwargs):
        self.dataService.constructCalibrationStateRoot = self.old_constructCalibrationStateRoot
        self.dataService.generateStateId = self.old_generateStateId
        self.tmpdir.cleanup()
        
        # For debugging purposes, you may want to re-add this line:
        # assert not self.tmppath.exists()        
        # OTHERWISE, I'm not going to _assume_ that you never want to mock-out `pathlib.Path.exists`!
        
        del self.tmpdir

    def path(self) -> Path:
        return self.tmppath

    def addFileAs(self, source: str, target: str):
        assert Path(self.tmpdir.name) in Path(target).parents
        
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        
        # For debugging purposes, you may want to re-add this line:
        # assert Path(target).exists()

    def saveObjectAt(self, thing: BaseModel, target: str):
        assert Path(self.tmpdir.name) in Path(target).parents
        
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        write_model_pretty(thing, target)
        
        # For debugging purposes, you may want to re-add this line:
        # assert Path(target).exists()


class reduction_root_redirect:
    """
    This context manager will create a temporary directory and patch a LocalDataService so that its
    reduction state root directory points inside the temporary directory.  Files can be easily added to the
    directory using `addFileAs`.  Usage is

    ```
    with reduction_root_redirect(instance.dataService) as tmpRoot:
        <code here>
        tmpRoot.addFileAs(some_file, target_in_tmp_root)
        <more code here>
    ```
    """

    def __init__(self, dataService: LocalDataService, *, stateId: str = None):
        self.dataService = dataService
        self.stateId = stateId
        self.old_constructReductionStateRoot = dataService._constructReductionStateRoot
        self.old_generateStateId = dataService.generateStateId

    def __enter__(self):
        self.tmpdir = TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/")
        self.tmppath = Path(self.tmpdir.name)
        if self.stateId is not None:
            self.tmppath = self.tmppath / Path(self.stateId)
        else:
            self.stateId = str(self.tmppath.parts[-1])
        self.dataService.generateStateId = lambda *x, **y: (self.stateId, None)  # noqa ARG005
        self.dataService._constructReductionStateRoot = lambda *x, **y: self.tmppath  # noqa ARG005
        return self

    def __exit__(self, *arg, **kwargs):
        self.dataService._constructReductionStateRoot = self.old_constructReductionStateRoot
        self.dataService.generateStateId = self.old_generateStateId
        self.tmpdir.cleanup()
        
        # For debugging purposes, you may want to re-add this line:
        # assert not self.tmppath.exists()
        
        del self.tmpdir

    def path(self) -> Path:
        return self.tmppath

    def addFileAs(self, source: str, target: str):
        assert self.tmppath in list(Path(target).parents)
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        
        # For debugging purposes, you may want to re-add this line:
        # assert Path(target).exists()

import shutil
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest
from snapred.backend.data.LocalDataService import LocalDataService

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
    stateId, _ = dataService._generateStateId(runNumber)
    stateRoot = Path(dataService._constructStateRoot(stateId))
    if stateRoot.exists():
        raise RuntimeError(f"state-root directory '{stateRoot}' already exists -- please move it out of the way!")

    dataService.initializeState(runNumber, useLiteMode, name)
    yield stateRoot

    # __exit__
    if delete_at_exit:
        shutil.rmtree(stateRoot)


@pytest.fixture()
def state_root_fixture():
    _stack = ExitStack()

    def _state_root_fixture(runNumber: str, name: str, useLiteMode: bool = False, delete_at_exit=True):
        return _stack.enter_context(state_root_override(runNumber, name, useLiteMode, delete_at_exit))

    yield _state_root_fixture

    # teardown => __exit__
    _stack.close()
